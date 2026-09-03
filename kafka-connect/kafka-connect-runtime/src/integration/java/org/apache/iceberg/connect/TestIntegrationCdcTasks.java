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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/**
 * How the changes of one key behave when they are split over the two tasks of the connector. The
 * test topic has two partitions and the connector runs two tasks, so a partition maps to a task.
 *
 * <p>A key must be produced to one partition. When two tasks write the same key within one commit,
 * each writes its own row and the equality delete of the later change cannot remove the row of the
 * other task, because both rows carry the sequence number of that commit. Across commits the change
 * is applied whichever task wrote the earlier row.
 *
 * <p>A task only joins the commit protocol once it has received a record, and the coordinator waits
 * for every partition (the tests disable the commit timeout). Each scenario therefore also sends an
 * unrelated row to the other partition.
 */
public class TestIntegrationCdcTasks extends CdcIntegrationTestBase {

  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, "cdc_tasks");

  // long enough for both partitions to be consumed before the first commit
  private static final int COMMIT_INTERVAL_MS = 15_000;

  @Test
  public void testSameKeyInOnePartitionWithinOneCommit() {
    createTable();
    startCdcConnector(createConfig(false));

    // the same task sees both changes and removes the first row within the batch
    sendEvent(0, event(1, "a", "v1", "I"));
    sendEvent(0, event(1, "a", "v2", "U"));
    sendEvent(1, event(99, "a", "v1", "I"));
    flushEvents();

    awaitSnapshots(TABLE_IDENTIFIER, 1, Duration.ofSeconds(60));
    awaitRows(TABLE_IDENTIFIER, expected("1|v2", "99|v1"), "id", "payload");
    assertThat(catalog().loadTable(TABLE_IDENTIFIER).snapshots()).hasSize(1);
    // the position delete written by the task for the first row, no equality delete
    assertThat(allDeleteFiles(catalog().loadTable(TABLE_IDENTIFIER))).hasSize(1);
  }

  @Test
  public void testSameKeyAcrossPartitionsWithinOneCommitKeepsBothRows() {
    createTable();
    startCdcConnector(createConfig(false));

    // two tasks each write a row for key 1 into the same commit
    sendEvent(0, event(1, "a", "v1", "I"));
    sendEvent(1, event(1, "a", "v2", "U"));
    flushEvents();

    awaitSnapshots(TABLE_IDENTIFIER, 1, Duration.ofSeconds(60));
    // the equality delete of the update finds no earlier commit to apply to, so both rows remain:
    // records of one key have to be produced to one partition
    awaitRows(TABLE_IDENTIFIER, expected("1|v1", "1|v2"), "id", "payload");
    assertThat(catalog().loadTable(TABLE_IDENTIFIER).snapshots()).hasSize(1);
  }

  @Test
  public void testSameKeyAcrossPartitionsInLaterCommit() {
    createTable();
    startCdcConnector(createConfig(false));

    sendEvent(0, event(1, "a", "v1", "I"));
    sendEvent(1, event(99, "a", "v1", "I"));
    flushEvents();
    awaitSnapshots(TABLE_IDENTIFIER, 1, Duration.ofSeconds(60));
    awaitRows(TABLE_IDENTIFIER, expected("1|v1", "99|v1"), "id", "payload");

    // the update arrives at the other task after the insert was committed
    sendEvent(1, event(1, "a", "v2", "U"));
    sendEvent(0, event(98, "a", "v1", "I"));
    flushEvents();
    awaitSnapshots(TABLE_IDENTIFIER, 2, Duration.ofSeconds(60));
    awaitRows(TABLE_IDENTIFIER, expected("1|v2", "98|v1", "99|v1"), "id", "payload");
    assertOnlyDeletionVectors(TABLE_IDENTIFIER);
  }

  private void createTable() {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            TestEvent.TEST_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return cdcConfig(TABLE_IDENTIFIER)
        .config("iceberg.control.commit.interval-ms", COMMIT_INTERVAL_MS);
  }

  @Override
  void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }
}
