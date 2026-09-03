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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Helpers for CDC scenarios: events as schemaless JSON with an {@code op} field, a producer that
 * can target a partition, and checks on the rows and delete files of the table.
 */
public abstract class CdcIntegrationTestBase extends IntegrationTestBase {

  protected static final String CONVERTED_EQ_DELETE_FILES_PROP =
      "kafka.connect.converted-equality-delete-files";

  private KafkaProducer<String, String> cdcProducer;

  @BeforeEach
  public void cdcBefore() {
    this.cdcProducer = context().initLocalProducer();
  }

  @AfterEach
  public void cdcAfter() {
    cdcProducer.close();
  }

  /** Connector config for one table with the CDC field and equality delete conversion enabled. */
  protected KafkaConnectUtils.Config cdcConfig(TableIdentifier tableIdentifier) {
    return createCommonConfig(false)
        .config("iceberg.tables", tableIdentifier.toString())
        .config("iceberg.tables.cdc-field", "op")
        .config("iceberg.tables.convert-equality-deletes-enabled", true);
  }

  protected void startCdcConnector(KafkaConnectUtils.Config config) {
    context().connectorCatalogProperties().forEach(config::config);
    context().startConnector(config);
  }

  protected Map<String, Object> event(long id, String type, String payload, String op) {
    Map<String, Object> event = Maps.newLinkedHashMap();
    event.put("id", id);
    event.put("type", type);
    event.put("ts", Instant.now().toEpochMilli());
    event.put("payload", payload);
    event.put("op", op);
    return event;
  }

  /** Sends the event keyed by its id, so that one key always lands in one partition. */
  protected void sendEvent(Map<String, Object> event) {
    sendEvent(null, event);
  }

  /** Sends the event to the given partition, or by key when the partition is null. */
  protected void sendEvent(Integer partition, Map<String, Object> event) {
    try {
      String json = TestContext.MAPPER.writeValueAsString(event);
      cdcProducer
          .send(new ProducerRecord<>(testTopic(), partition, event.get("id").toString(), json))
          .get();
    } catch (JsonProcessingException | InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  protected void flushEvents() {
    cdcProducer.flush();
  }

  /** Rows of the table as {@code field|field|...} strings. */
  protected Set<String> rows(TableIdentifier tableIdentifier, String... fields) {
    Table table = catalog().loadTable(tableIdentifier);
    Set<String> rows = Sets.newHashSet();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        List<String> values = Lists.newArrayList();
        for (String field : fields) {
          Object value = record.struct().field(field) == null ? null : record.getField(field);
          values.add(String.valueOf(value));
        }
        rows.add(String.join("|", values));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return rows;
  }

  protected void awaitRows(
      TableIdentifier tableIdentifier, Set<String> expected, String... fields) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(rows(tableIdentifier, fields)).isEqualTo(expected));
  }

  protected void awaitSnapshots(TableIdentifier tableIdentifier, int count, Duration timeout) {
    Awaitility.await()
        .atMost(timeout)
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> Iterables.size(catalog().loadTable(tableIdentifier).snapshots()) >= count);
  }

  protected List<DeleteFile> allDeleteFiles(Table table) {
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
      changes.addedDeleteFiles().forEach(deleteFiles::add);
    }

    return deleteFiles;
  }

  /** Every delete file in the table is a deletion vector and at least one commit converted. */
  protected void assertOnlyDeletionVectors(TableIdentifier tableIdentifier) {
    Table table = catalog().loadTable(tableIdentifier);
    List<DeleteFile> deleteFiles = allDeleteFiles(table);
    assertThat(deleteFiles).isNotEmpty().allMatch(ContentFileUtil::isDV);
    assertThat(table.snapshots())
        .anyMatch(snapshot -> snapshot.summary().containsKey(CONVERTED_EQ_DELETE_FILES_PROP));
  }

  protected static Set<String> expected(String... rows) {
    return Sets.newHashSet(rows);
  }

  protected static String uniqueName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }

  @Override
  protected void sendEvents(boolean useSchema) {
    // the scenarios send their own events
  }
}
