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

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

/**
 * CDC on a partitioned table. The partition column is part of the identifier columns, so a delete
 * is written into the partition of the row it removes and the conversion pins that partition.
 */
public class TestIntegrationCdcPartitioned extends CdcIntegrationTestBase {

  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, "cdc_part");

  @Test
  public void testDeletesStayWithinTheirPartition() {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            TestEvent.TEST_SCHEMA,
            PartitionSpec.builderFor(TestEvent.TEST_SCHEMA).identity("type").build(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    startCdcConnector(createConfig(false));

    // first commit: two rows in each partition
    sendEvent(event(1, "a", "v1", "I"));
    sendEvent(event(2, "a", "v1", "I"));
    sendEvent(event(3, "b", "v1", "I"));
    sendEvent(event(4, "b", "v1", "I"));
    flushEvents();
    awaitRows(
        TABLE_IDENTIFIER,
        expected("1|a|v1", "2|a|v1", "3|b|v1", "4|b|v1"),
        "id",
        "type",
        "payload");

    // later commit: update in partition a, delete in partition b, insert in partition b
    sendEvent(event(1, "a", "v2", "U"));
    sendEvent(event(3, "b", "v1", "D"));
    sendEvent(event(5, "b", "v1", "I"));
    flushEvents();
    awaitRows(
        TABLE_IDENTIFIER,
        expected("1|a|v2", "2|a|v1", "4|b|v1", "5|b|v1"),
        "id",
        "type",
        "payload");

    assertOnlyDeletionVectors(TABLE_IDENTIFIER);

    // each vector removes one row and belongs to the partition of the data file it references
    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    List<DeleteFile> vectors = allDeleteFiles(table);
    assertThat(vectors).hasSize(2);
    assertThat(vectors).allMatch(vector -> vector.recordCount() == 1L);
    assertThat(vectors.stream().map(vector -> vector.partition().get(0, String.class)))
        .containsExactlyInAnyOrder("a", "b");

    List<DataFile> dataFiles = Lists.newArrayList();
    table.newScan().planFiles().forEach(task -> dataFiles.add(task.file()));
    for (DeleteFile vector : vectors) {
      DataFile referenced =
          dataFiles.stream()
              .filter(file -> file.location().equals(vector.referencedDataFile()))
              .findFirst()
              .orElseThrow();
      assertThat(referenced.partition().get(0, String.class))
          .isEqualTo(vector.partition().get(0, String.class));
    }

    assertThat(Sets.newHashSet(rows(TABLE_IDENTIFIER, "type"))).containsExactlyInAnyOrder("a", "b");
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    // the partition column must be an identifier column for CDC on a partitioned table
    return cdcConfig(TABLE_IDENTIFIER).config("iceberg.tables.default-id-columns", "id,type");
  }

  @Override
  void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }
}
