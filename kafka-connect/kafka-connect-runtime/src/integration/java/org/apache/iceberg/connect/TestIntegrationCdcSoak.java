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
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Longer CDC run: many commits of inserts, updates of recent keys and deletes. Checks the final
 * rows against a model of the events, that no equality delete file ever reached the table, and,
 * from the worker logs, that a conversion reads a fraction of the table's data files.
 *
 * <p>Takes several minutes, so it only runs when the environment variable {@code KC_SOAK=true} is
 * set: {@code KC_SOAK=true ./gradlew
 * :iceberg-kafka-connect:iceberg-kafka-connect-runtime:integrationTest --tests
 * '*TestIntegrationCdcSoak*'}.
 */
@EnabledIfEnvironmentVariable(named = "KC_SOAK", matches = "true")
public class TestIntegrationCdcSoak extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestIntegrationCdcSoak.class);

  private static final String TEST_TABLE = "cdc_soak";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);
  private static final String CONVERTED_EQ_DELETE_FILES_PROP =
      "kafka.connect.converted-equality-delete-files";
  private static final Pattern SCANNED_FILES =
      Pattern.compile("scanned (\\d+) data file\\(s\\) in (\\d+) ms");

  private static final int ROUNDS = 40;
  private static final int INSERTS_PER_ROUND = 50;
  private static final int UPDATES_PER_ROUND = 50;
  private static final int DELETES_PER_ROUND = 10;
  // updates and deletes target keys inserted in the last rounds, like a CDC stream that changes
  // recent rows most often
  private static final int RECENT_ROUNDS = 3;

  private final Instant now = Instant.now();

  @Test
  public void testManyCommits() {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            TestEvent.TEST_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    KafkaConnectUtils.Config connectorConfig = createConfig(false);
    context().connectorCatalogProperties().forEach(connectorConfig::config);
    context().startConnector(connectorConfig);

    // model of the table: key -> payload of the live row
    Map<Long, String> expected = Maps.newHashMap();
    Random random = new Random(42L);
    long nextId = 1;
    for (int round = 0; round < ROUNDS; round++) {
      long firstRecent = Math.max(1, nextId - (long) RECENT_ROUNDS * INSERTS_PER_ROUND);
      for (int i = 0; i < INSERTS_PER_ROUND; i++) {
        long id = nextId++;
        String payload = "r" + round;
        expected.put(id, payload);
        send(testTopic(), new TestEvent(id, "type", now, payload, "I"), false);
      }

      for (int i = 0; i < UPDATES_PER_ROUND; i++) {
        long id = firstRecent + random.nextInt((int) (nextId - firstRecent));
        if (expected.containsKey(id)) {
          String payload = "u" + round;
          expected.put(id, payload);
          send(testTopic(), new TestEvent(id, "type", now, payload, "U"), false);
        }
      }

      for (int i = 0; i < DELETES_PER_ROUND; i++) {
        long id = firstRecent + random.nextInt((int) (nextId - firstRecent));
        if (expected.remove(id) != null) {
          send(testTopic(), new TestEvent(id, "type", now, "", "D"), false);
        }
      }

      flush();
      // one commit per round, so that updates and deletes of earlier rounds hit committed rows
      awaitNewSnapshot();
    }

    Set<String> expectedRows = Sets.newHashSet();
    expected.forEach((id, payload) -> expectedRows.add(id + "|" + payload));
    LOG.info("Sent {} rounds, expecting {} live rows", ROUNDS, expectedRows.size());

    Awaitility.await()
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(rows()).isEqualTo(expectedRows));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    int snapshots = 0;
    int snapshotsWithDeletes = 0;
    int convertedSnapshots = 0;
    for (Snapshot snapshot : table.snapshots()) {
      snapshots++;
      SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
      List<DeleteFile> added = Lists.newArrayList(changes.addedDeleteFiles());
      deleteFiles.addAll(added);
      if (!added.isEmpty()) {
        snapshotsWithDeletes++;
      }
      if (snapshot.summary().containsKey(CONVERTED_EQ_DELETE_FILES_PROP)) {
        convertedSnapshots++;
      }
    }

    Map<String, String> summary = table.currentSnapshot().summary();
    long totalDataFiles = Long.parseLong(summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    long totalDeleteFiles =
        Long.parseLong(summary.getOrDefault(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0"));
    LOG.info(
        "{} snapshots, {} with deletes, {} converted, {} data files, {} delete files",
        snapshots,
        snapshotsWithDeletes,
        convertedSnapshots,
        totalDataFiles,
        totalDeleteFiles);

    // every delete that reached the table is a deletion vector
    assertThat(snapshots).isGreaterThan(1);
    assertThat(deleteFiles).isNotEmpty().allMatch(ContentFileUtil::isDV);
    assertThat(convertedSnapshots).isGreaterThan(0);
    // one deletion vector per data file at most
    assertThat(totalDeleteFiles).isLessThanOrEqualTo(totalDataFiles);
    assertSnapshotProps(TABLE_IDENTIFIER, null);

    // the worker logs one line per conversion; recent keys prune to a few of the data files
    List<int[]> conversions = conversionsFromLogs();
    assertThat(conversions).isNotEmpty();
    int maxScanned = conversions.stream().mapToInt(c -> c[0]).max().orElse(0);
    LOG.info(
        "{} conversions logged, at most {} data files scanned per conversion of {} total",
        conversions.size(),
        maxScanned,
        totalDataFiles);
    assertThat(maxScanned).isLessThan((int) totalDataFiles);
  }

  private void awaitNewSnapshot() {
    Long before = currentSnapshotId();
    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(250))
        .until(() -> !Objects.equals(currentSnapshotId(), before));
  }

  private Long currentSnapshotId() {
    Snapshot snapshot = catalog().loadTable(TABLE_IDENTIFIER).currentSnapshot();
    return snapshot == null ? null : snapshot.snapshotId();
  }

  /** Returns [scanned data files, millis] for every conversion the worker logged. */
  private List<int[]> conversionsFromLogs() {
    List<int[]> conversions = Lists.newArrayList();
    Matcher matcher = SCANNED_FILES.matcher(context().connectLogs());
    while (matcher.find()) {
      conversions.add(
          new int[] {Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2))});
    }

    return conversions;
  }

  private Set<String> rows() {
    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    Set<String> rows = Sets.newHashSet();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(record.getField("id") + "|" + record.getField("payload"));
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
    // events are sent by the test itself
  }

  @Override
  void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }
}
