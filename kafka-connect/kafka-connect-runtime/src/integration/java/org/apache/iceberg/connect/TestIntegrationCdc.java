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
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIntegrationCdc extends IntegrationTestBase {

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

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema, ImmutableMap.of(), List.of(TABLE_IDENTIFIER));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    List<DeleteFile> deleteFiles = deleteFiles(TABLE_IDENTIFIER, branch);

    // DataFiles: 3 inserts + 1 update write = 4 records
    // 2 partitions (today + 3 days ago) × up to 2 workers = 2~4 files
    assertThat(files).hasSizeBetween(2, 4);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(4);

    // DeleteFiles: 1 delete + 1 update delete = 2 records
    assertThat(deleteFiles).hasSizeBetween(1, 4);
    assertThat(deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(2);

    // Actual scan result: id=2 and id=3 (updated) = 2 records
    assertThat(scanActualRecordCount(TABLE_IDENTIFIER)).isEqualTo(2);

    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkUnpartitionedTable(String branch) {
    catalog().createTable(TABLE_IDENTIFIER, TestCdcEvent.TEST_SCHEMA);

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema, ImmutableMap.of(), List.of(TABLE_IDENTIFIER));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    List<DeleteFile> deleteFiles = deleteFiles(TABLE_IDENTIFIER, branch);

    // DataFiles: 3 inserts + 1 update write = 4 records
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(4);

    // DeleteFiles: 1 delete + 1 update delete = 2 records
    assertThat(deleteFiles).hasSizeBetween(1, 2);
    assertThat(deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(2);

    // Actual scan result: id=2 and id=3 (updated) = 2 records
    assertThat(scanActualRecordCount(TABLE_IDENTIFIER)).isEqualTo(2);

    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkSchemaEvolution(String branch) {
    // CDC 스키마 진화 테스트: 전체 스키마 구조를 미리 생성하되 id만 IntegerType으로 설정
    Schema initialSchema =
        new Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "type", StringType.get()),
                Types.NestedField.required(3, "data", StringType.get()),
                //                Types.NestedField.required(4, "ts",
                // Types.TimestampType.withZone()), // useSchema 여부에 따라 스키마 진화로 추가될 필드
                Types.NestedField.required(5, "payload", StringType.get()),
                Types.NestedField.required(6, "$__source", TestCdcEvent.$__SOURCE),
                Types.NestedField.required(26, "_cdc", TestCdcEvent.$_CDC)),
            ImmutableSet.of(1));
    catalog().createTable(TABLE_IDENTIFIER, initialSchema);

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(
        branch,
        useSchema,
        ImmutableMap.of("iceberg.tables.evolve-schema-enabled", "true"),
        List.of(TABLE_IDENTIFIER));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    List<DeleteFile> deleteFiles = deleteFiles(TABLE_IDENTIFIER, branch);

    // DataFiles: 3 inserts + 1 update write = 4 records
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(4);

    // DeleteFiles: 1 delete + 1 update delete = 2 records
    assertThat(deleteFiles).hasSizeBetween(1, 2);
    assertThat(deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(2);

    // Actual scan result: 2 records
    assertThat(scanActualRecordCount(TABLE_IDENTIFIER)).isEqualTo(2);

    assertSnapshotProps(TABLE_IDENTIFIER, branch);

    // when not using a value schema, the ID data type will not be updated
    Class<? extends Type> expectedIdType = useSchema ? LongType.class : Types.IntegerType.class;

    assertGeneratedSchema(useSchema, expectedIdType);
  }

  private void assertGeneratedSchema(boolean useSchema, Class<? extends Type> expectedIdType) {
    Schema tableSchema = catalog().loadTable(TABLE_IDENTIFIER).schema();
    assertThat(tableSchema.findField("id").type()).isInstanceOf(expectedIdType);
    assertThat(tableSchema.findField("type").type()).isInstanceOf(StringType.class);
    assertThat(tableSchema.findField("payload").type()).isInstanceOf(StringType.class);
    // _cdc struct should always be present
    assertThat(tableSchema.findField("_cdc")).isNotNull();

    if (!useSchema) {
      // without a schema we can only map the primitive type
      assertThat(tableSchema.findField("ts").type()).isInstanceOf(LongType.class);
    } else {
      assertThat(tableSchema.findField("ts").type()).isInstanceOf(TimestampType.class);
    }
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE))
        .config("iceberg.tables.cdc-field", "_cdc.op")
        .config("transforms", "debezium")
        .config(
            "transforms.debezium.type", "org.apache.iceberg.connect.transforms.DebeziumTransform");
  }

  @Override
  protected void runIntegrationFlow(boolean useSchema) {
    // start with 3 records, update 1, delete 1. Should be a total of 4 adds and 2 deletes
    // (the update will be 1 add and 1 delete)

    // CDC 전용 source 및 header 구조체 생성
    var sourceStruct = TestCdcEvent.createSourceStruct();
    var dmlHeader =
        TestCdcEvent.createDmlHeader("75244835-b5f9-11ef-8069-fa163e2b0186:191646", "11424", "0");

    TestCdcEvent event1 =
        new TestCdcEvent(
            1, "type1", "data1", Instant.now(), "hello world!", "c", sourceStruct, dmlHeader);
    TestCdcEvent event2 =
        new TestCdcEvent(
            2, "type2", "data2", Instant.now(), "having fun?", "c", sourceStruct, dmlHeader);

    Instant threeDaysAgo = Instant.now().minus(Duration.ofDays(3));
    TestCdcEvent event3 =
        new TestCdcEvent(
            3,
            "type3",
            "data3",
            threeDaysAgo,
            "hello from the past!",
            "c",
            sourceStruct,
            dmlHeader);

    TestCdcEvent event4 =
        new TestCdcEvent(
            1, "type1", "data1", Instant.now(), "hello world!", "d", sourceStruct, dmlHeader);
    TestCdcEvent event5 =
        new TestCdcEvent(
            3, "type3", "data3", threeDaysAgo, "updated!", "u", sourceStruct, dmlHeader);

    send(testTopic(), event1, useSchema);
    send(testTopic(), event2, useSchema);
    send(testTopic(), event3, useSchema);
    send(testTopic(), event4, useSchema);
    send(testTopic(), event5, useSchema);

    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(List.of(TABLE_IDENTIFIER), 1));
  }

  @Override
  void dropTables() {
    catalog().dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
  }
}
