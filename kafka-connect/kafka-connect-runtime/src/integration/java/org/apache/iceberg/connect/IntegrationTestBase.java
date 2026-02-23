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
import static org.assertj.core.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Struct;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBase.class.getName());

  protected static TestContext context;

  private Catalog catalog;
  private Admin admin;
  private String connectorName;
  private String testTopic;
  private String branch;

  private KafkaProducer<String, String> producer;

  protected static final int TEST_TOPIC_PARTITIONS = 2;
  protected static final String TEST_DB = "test";

  abstract KafkaConnectUtils.Config createConfig(boolean useSchema);

  abstract void runIntegrationFlow(boolean useSchema);

  abstract void dropTables();

  protected TestContext context() {
    return context;
  }

  protected Catalog catalog() {
    return catalog;
  }

  protected String connectorName() {
    return connectorName;
  }

  protected String testTopic() {
    return testTopic;
  }

  @BeforeAll
  public static void baseBeforeAll() {
    context = TestContext.instance();
  }

  @BeforeEach
  public void baseBefore() {
    this.catalog = context.initLocalCatalog();
    this.producer = context.initLocalProducer();
    this.admin = context.initLocalAdmin();
    this.connectorName = "test_connector-" + UUID.randomUUID();
    this.testTopic = "test-topic-" + UUID.randomUUID();
    createTopic(testTopic(), TEST_TOPIC_PARTITIONS);
    ((SupportsNamespaces) catalog()).createNamespace(Namespace.of(TEST_DB));
  }

  @AfterEach
  public void baseAfter() {
    context().stopConnector(connectorName());
    deleteTopic(testTopic());
    dropTables();
    ((SupportsNamespaces) catalog()).dropNamespace(Namespace.of(TEST_DB));
    try {
      if (catalog instanceof AutoCloseable) {
        ((AutoCloseable) catalog).close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    producer.close();
    admin.close();
  }

  protected void assertSnapshotProps(TableIdentifier tableIdentifier, String branch) {
    Table table = catalog.loadTable(tableIdentifier);
    Map<String, String> props = latestSnapshot(table, branch).summary();
    assertThat(props)
        .hasKeySatisfying(
            new Condition<>() {
              @Override
              public boolean matches(String str) {
                return str.startsWith("kafka.connect.offsets.");
              }
            });
    assertThat(props).containsKey("kafka.connect.commit-id");
    assertThat(props).containsKey("kafka.connect.task-id");
  }

  protected List<DataFile> dataFiles(TableIdentifier tableIdentifier, String branch) {
    Table table = catalog.loadTable(tableIdentifier);
    return Lists.newArrayList(latestSnapshot(table, branch).addedDataFiles(table.io()));
  }

  protected List<DeleteFile> deleteFiles(TableIdentifier tableIdentifier, String branch) {
    Table table = catalog.loadTable(tableIdentifier);
    return Lists.newArrayList(latestSnapshot(table, branch).addedDeleteFiles(table.io()));
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    return branch == null ? table.currentSnapshot() : table.snapshot(branch);
  }

  protected void createTopic(String topicName, int partitions) {
    try {
      admin
          .createTopics(ImmutableList.of(new NewTopic(topicName, partitions, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  protected void deleteTopic(String topicName) {
    try {
      admin.deleteTopics(ImmutableList.of(topicName)).all().get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  protected void send(String topicName, TestEventBase event, boolean useSchema) {
    String eventStr = event.serialize(useSchema);
    try {
      ProducerRecord<String, String> record =
          new ProducerRecord<>(topicName, Long.toString(event.id()), eventStr);

      // Add headers from event.header()
      Struct header = event.header();
      if (header != null) {
        header
            .schema()
            .fields()
            .forEach(
                field -> {
                  Object value = header.get(field);
                  if (value != null) {
                    record
                        .headers()
                        .add(field.name(), value.toString().getBytes(StandardCharsets.UTF_8));
                  }
                });
      }

      producer.send(record).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  protected void flush() {
    producer.flush();
  }

  protected KafkaConnectUtils.Config createCommonConfig(boolean useSchema) {
    // set offset reset to the earliest, so we don't miss any test messages
    return new KafkaConnectUtils.Config(connectorName())
        .config("topics", testTopic())
        .config("connector.class", IcebergSinkConnector.class.getName())
        .config("tasks.max", 2)
        .config("consumer.override.auto.offset.reset", "earliest")
        .config("key.converter", "org.apache.kafka.connect.json.JsonConverter")
        .config("key.converter.schemas.enable", false)
        .config("value.converter", "org.apache.kafka.connect.json.JsonConverter")
        .config("value.converter.schemas.enable", useSchema)
        .config("iceberg.control.commit.interval-ms", 10000)
        .config("iceberg.control.commit.timeout-ms", Integer.MAX_VALUE)
        .config("iceberg.kafka.auto.offset.reset", "earliest")
        .config("iceberg.hdfs.authentication.kerberos", false)
        .config("iceberg.hadoop-cluster-name", "test-cluster")
        .config("connect.admin.url", context().getApiServerUrlForContainer())
        .config("cmdb.api.url", context().getApiServerUrlForContainer())
        .config("cmdb.api.token", "test-token");
  }

  protected void runTest(
      String branch,
      boolean useSchema,
      Map<String, String> extraConfig,
      List<TableIdentifier> tableIdentifiers) {
    KafkaConnectUtils.Config connectorConfig = createConfig(useSchema);

    context().connectorCatalogProperties().forEach(connectorConfig::config);

    if (branch != null) {
      connectorConfig.config("iceberg.tables.default-commit-branch", branch);
      this.branch = branch;
    }

    extraConfig.forEach(connectorConfig::config);

    context().startConnector(connectorConfig);

    runIntegrationFlow(useSchema);
  }

  protected void assertSnapshotAdded(
      List<TableIdentifier> tableIdentifiers, int expectedSnapshots) {
    for (TableIdentifier tableId : tableIdentifiers) {
      try {
        Table table = catalog().loadTable(tableId);
        assertThat(table.snapshots()).hasSize(expectedSnapshots);
      } catch (NoSuchTableException e) {
        fail("Table should exist");
      }
    }
  }

  /**
   * Scan the table and count actual readable records (with delete files applied). This performs a
   * full table scan and reads all records, applying delete files. This is the true count that a
   * SELECT query would return. Supports reading from a specific branch by passing the branch name.
   */
  protected long scanActualRecordCount(TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);

    long count = 0;
    try {
      // Use IcebergGenerics to read actual records with delete files applied
      IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);

      // Apply branch/snapshot if specified
      if (branch != null) {
        Snapshot snapshot = table.snapshot(branch);
        if (snapshot != null) {
          scanBuilder = scanBuilder.useSnapshot(snapshot.snapshotId());
        }
      }

      // Iterate through all records (delete files are automatically applied)
      try (CloseableIterable<Record> records = scanBuilder.build()) {
        for (org.apache.iceberg.data.Record record : records) {
          count++;
          LOG.info(
              "  Scanned record #{}: id={}, type={}, data={}",
              count,
              record.getField("id"),
              record.getField("type"),
              record.getField("data"));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to scan table for record count", e);
    }

    return count;
  }

  /**
   * Log detailed table analysis including DataFiles, DeleteFiles, Snapshot summary, and actual
   * record count. Returns the scanned record count for assertion purposes.
   */
  protected long logTableAnalysis(TableIdentifier tableIdentifier, String analysisTitle) {
    List<DataFile> files = dataFiles(tableIdentifier, branch);
    List<DeleteFile> deleteFiles = deleteFiles(tableIdentifier, branch);

    LOG.info("========== {} ==========", analysisTitle);
    LOG.info("DataFiles count: {}", files.size());
    files.forEach(
        file ->
            LOG.info(
                "  DataFile: path={}, recordCount={}, partition={}",
                file.path(),
                file.recordCount(),
                file.partition()));
    LOG.info("Total DataFile records: {}", files.stream().mapToLong(DataFile::recordCount).sum());

    LOG.info("DeleteFiles count: {}", deleteFiles.size());
    deleteFiles.forEach(
        file ->
            LOG.info(
                "  DeleteFile: path={}, recordCount={}, partition={}",
                file.path(),
                file.recordCount(),
                file.partition()));
    LOG.info(
        "Total DeleteFile records: {}",
        deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum());

    Table table = catalog.loadTable(tableIdentifier);
    Snapshot snapshot = latestSnapshot(table, branch);
    LOG.info("Snapshot summary: {}", snapshot.summary());

    long scannedRecordCount = scanActualRecordCount(tableIdentifier);
    LOG.info("Scanned actual record count (after applying deletes): {}", scannedRecordCount);
    LOG.info("=".repeat(analysisTitle.length() + 22));

    return scannedRecordCount;
  }
}
