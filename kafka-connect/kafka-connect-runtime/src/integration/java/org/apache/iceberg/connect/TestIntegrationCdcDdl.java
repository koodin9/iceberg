/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Struct;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIntegrationCdcDdl extends IntegrationTestBase {

  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);

  @BeforeEach
  public void before() {
    // Reset API server handlers before each test to ensure clean state
    context.getApiServer().resetHandlers();

    // Initialize DDL execution state for the test table
    context.initializeDdlExecution(
        0,
        1L,
        "CREATE TABLE " + context.getFullTableName(TEST_DB, TEST_TABLE),
        "Initial test data",
        TEST_DB,
        TEST_TABLE);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkPartitionedTable(String branch) {
    catalog().createTable(TABLE_IDENTIFIER, TestCdcEvent.TEST_SCHEMA, TestCdcEvent.TEST_SPEC);

    // DDL 처리 시 Worker.java에서 ddl을 가져올때 Struct 타입으로 캐스팅하므로 schema 필수
    // TODO: Struct 타입 캐스팅 없어도 ddl을 가져올 수 있도록 개선 필요
    // (실제 환경의 AvroConverter도 항상 Struct 반환)
    boolean useSchema = true;

    runTest(branch, useSchema, ImmutableMap.of(), List.of(TABLE_IDENTIFIER));

    long scannedRecordCount = logTableAnalysis(TABLE_IDENTIFIER, "Partitioned Table Analysis");

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    List<DeleteFile> deleteFiles = deleteFiles(TABLE_IDENTIFIER, branch);

    // partition may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    // truncate 이후 event1V2(I) + event2V2(I) + event4V2(D)
    // DataFile에는 2개의 물리적 레코드 존재 (id=1, id=2)
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);

    // DeleteFile이 1개의 레코드를 마킹 (id=1)
    assertThat(deleteFiles).hasSizeBetween(1, 2);
    assertThat(deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(1);

    // 실제 스캔 결과는 1개 (id=2만 남음, id=1은 삭제됨)
    assertThat(scannedRecordCount).isEqualTo(1);

    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkUnpartitionedTable(String branch) {
    catalog().createTable(TABLE_IDENTIFIER, TestCdcEvent.TEST_SCHEMA);

    // DDL 처리 시 Worker.java에서 ddl을 가져올때 Struct 타입으로 캐스팅하므로 schema 필수
    // (실제 환경의 AvroConverter도 항상 Struct 반환)
    boolean useSchema = true;
    runTest(branch, useSchema, ImmutableMap.of(), List.of(TABLE_IDENTIFIER));
    long scannedRecordCount = logTableAnalysis(TABLE_IDENTIFIER, "Unpartitioned Table Analysis");

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    List<DeleteFile> deleteFiles = deleteFiles(TABLE_IDENTIFIER, branch);

    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    // truncate 이후 event1V2(I) + event2V2(I) + event4V2(D)
    // DataFile에는 2개의 물리적 레코드 존재 (id=1, id=2)
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);

    // DeleteFile이 1개의 레코드를 마킹 (id=1)
    assertThat(deleteFiles).hasSizeBetween(1, 2);
    assertThat(deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(1);

    // 실제 스캔 결과는 1개 (id=2만 남음, id=1은 삭제됨)
    assertThat(scannedRecordCount).isEqualTo(1);

    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE))
        .config("iceberg.tables.cdc-field", "_cdc.op")
        .config("iceberg.kakao.cdc.enabled", true)
        .config("transforms", "debezium")
        .config(
            "transforms.debezium.type", "org.apache.iceberg.connect.transforms.DebeziumTransform");
  }

  @Override
  protected void runIntegrationFlow(boolean useSchema) {
    // CDC 전용 source 및 header 구조체 생성
    var sourceStruct = TestCdcEvent.createSourceStruct();
    var dmlHeader =
        TestCdcEvent.createDmlHeader("75244835-b5f9-11ef-8069-fa163e2b0186:191646", "11424", "0");

    // start with 3 records, update 1, delete 1. Should be a total of 4 adds and 2 deletes
    // (the update will be 1 add and 1 delete)
    TestCdcEvent event1 =
        new TestCdcEvent(
            1,
            "type1",
            "How can a column name be source?",
            Instant.now(),
            "hello world!",
            "c",
            sourceStruct,
            dmlHeader);
    TestCdcEvent event2 =
        new TestCdcEvent(
            2,
            "type2",
            "How can a column name be source?",
            Instant.now(),
            "having fun?",
            "c",
            sourceStruct,
            dmlHeader);

    Instant threeDaysAgo = Instant.now().minus(Duration.ofDays(3));

    TestCdcEvent event3 =
        new TestCdcEvent(
            3,
            "type3",
            "How can a column name be source?",
            threeDaysAgo,
            "hello from the past!",
            "c",
            sourceStruct,
            dmlHeader);

    TestCdcEvent event4 =
        new TestCdcEvent(
            1,
            "type1",
            "How can a column name be source?",
            Instant.now(),
            "hello world!",
            "d",
            sourceStruct,
            dmlHeader);
    TestCdcEvent event5 =
        new TestCdcEvent(
            3,
            "type3",
            "How can a column name be source?",
            threeDaysAgo,
            "updated!",
            "u",
            sourceStruct,
            dmlHeader);

    send(testTopic(), event1, useSchema);
    send(testTopic(), event2, useSchema);
    send(testTopic(), event3, useSchema);
    send(testTopic(), event4, useSchema);
    send(testTopic(), event5, useSchema);

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(3))
        .untilAsserted(
            () -> {
              assertSnapshotAdded(List.of(TABLE_IDENTIFIER), 1);
              assertThat(scanActualRecordCount(TABLE_IDENTIFIER)).isEqualTo(2);
            });

    // Send TRUNCATE DDL
    // DDL header의 last_dml_info에는 이전 버전(ddl_version=1)의 마지막 DML 정보가 들어감
    String lastDmlGtid = "75244835-b5f9-11ef-8069-fa163e2b0186:191646";
    String lastDmlPos = "11424";
    String lastDmlRow = "0";
    var ddlHeader = TestDdlEvent.createDdlHeader(lastDmlGtid, lastDmlPos, lastDmlRow);
    TestDdlEvent truncateEvent =
        new TestDdlEvent(
            4, Instant.now(), "cdcdb", TEST_TABLE, "truncate table " + TEST_TABLE, ddlHeader);

    // DDL 처리 전에 DML 상태가 CMDB에 기록되어 있어야 함
    // DDL header의 last_dml_info와 일치하는 값으로 설정
    context.initializeDmlStatus(
        0, // partition 0
        1L, // ddl_version 1 (DDL 이전 버전)
        lastDmlGtid,
        lastDmlPos,
        lastDmlRow,
        TEST_DB,
        TEST_TABLE);

    context.initializeDdlExecution(
        1,
        2L,
        "TRUNCATE TABLE " + context.getFullTableName(TEST_DB, TEST_TABLE),
        "Truncate test data",
        TEST_DB,
        TEST_TABLE);

    send(testTopic(), truncateEvent, useSchema);

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(3))
        .untilAsserted(
            () -> {
              assertSnapshotAdded(List.of(TABLE_IDENTIFIER), 2);
              assertThat(scanActualRecordCount(TABLE_IDENTIFIER)).isEqualTo(0);
            });

    // TRUNCATE 이후의 이벤트들은 ddl_version=2로 전송
    // event1V2(I) + event2V2(I) + event4V2(D) = 2 INSERT + 1 DELETE
    // 같은 커밋 사이클 내에서 id=1은 INSERT 후 DELETE되지만 물리적으로는 모두 기록됨
    // 결과: DataFile 2 records (id=1, id=2), DeleteFile 1 record (id=1 삭제 마커)
    // 실제 스캔 시: DeleteFile 적용되어 id=2만 읽힘
    Struct dmlHeaderV2 =
        TestCdcEvent.createDmlHeader(
            "75244835-b5f9-11ef-8069-fa163e2b0186:191646", "11424", "0", 2);
    TestCdcEvent event1V2 =
        new TestCdcEvent(
            1,
            "type1",
            "How can a column name be source?",
            Instant.now(),
            "hello world!",
            "c",
            sourceStruct,
            dmlHeaderV2);
    TestCdcEvent event2V2 =
        new TestCdcEvent(
            2,
            "type2",
            "How can a column name be source?",
            Instant.now(),
            "having fun?",
            "c",
            sourceStruct,
            dmlHeaderV2);
    TestCdcEvent event4V2 =
        new TestCdcEvent(
            1,
            "type1",
            "How can a column name be source?",
            Instant.now(),
            "hello world!",
            "d",
            sourceStruct,
            dmlHeaderV2);

    send(testTopic(), event1V2, useSchema);
    send(testTopic(), event2V2, useSchema);
    send(testTopic(), event4V2, useSchema);

    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(List.of(TABLE_IDENTIFIER), 3));
  }

  @Override
  void dropTables() {
    catalog().dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
  }
}
