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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Sends CDC events through a running connector and checks that updates and deletes of earlier
 * commits reach the table as deletion vectors, never as equality delete files.
 */
public class TestIntegrationCdc extends IntegrationTestBase {

  private static final String TEST_TABLE = "cdc";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);
  private static final String CONVERTED_EQ_DELETE_FILES_PROP =
      "kafka.connect.converted-equality-delete-files";

  private final Instant now = Instant.now();

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCdcDeletesAreConvertedToDeletionVectors(boolean useSchema) {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            TestEvent.TEST_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    KafkaConnectUtils.Config connectorConfig = createConfig(useSchema);
    context().connectorCatalogProperties().forEach(connectorConfig::config);
    context().startConnector(connectorConfig);

    // first commit: two inserts
    sendEvents(useSchema);
    flush();
    awaitRows("1|type1|v1", "2|type2|v1");

    // later commit: update one row, delete the other, insert a new one
    send(testTopic(), new TestEvent(1, "type1", now, "v2", "U"), useSchema);
    send(testTopic(), new TestEvent(2, "type2", now, "v1", "D"), useSchema);
    send(testTopic(), new TestEvent(3, "type3", now, "v1", "I"), useSchema);
    flush();
    awaitRows("1|type1|v2", "3|type3|v1");

    Table table = catalog().loadTable(TABLE_IDENTIFIER);

    // the update and the delete removed one row each, and both were written as deletion vectors
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    boolean converted = false;
    for (Snapshot snapshot : table.snapshots()) {
      SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
      changes.addedDeleteFiles().forEach(deleteFiles::add);
      converted |= snapshot.summary().containsKey(CONVERTED_EQ_DELETE_FILES_PROP);
    }

    assertThat(deleteFiles).isNotEmpty().allMatch(ContentFileUtil::isDV);
    assertThat(deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(2L);
    assertThat(converted).isTrue();
    assertSnapshotProps(TABLE_IDENTIFIER, null);
  }

  private void awaitRows(String... expected) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(rows()).containsExactlyInAnyOrder(expected));
  }

  private Set<String> rows() {
    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    Set<String> rows = Sets.newHashSet();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(
            record.getField("id")
                + "|"
                + record.getField("type")
                + "|"
                + record.getField("payload"));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return rows;
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE))
        .config("iceberg.tables.cdc-field", "op")
        .config("iceberg.tables.convert-equality-deletes-enabled", true);
  }

  @Override
  protected void sendEvents(boolean useSchema) {
    send(testTopic(), new TestEvent(1, "type1", now, "v1", "I"), useSchema);
    send(testTopic(), new TestEvent(2, "type2", now, "v1", "I"), useSchema);
  }

  @Override
  void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }
}
