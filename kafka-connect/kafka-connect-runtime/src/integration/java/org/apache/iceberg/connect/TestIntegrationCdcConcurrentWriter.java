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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

/**
 * CDC while another writer keeps appending to the table. The appends move the table between the
 * moment the conversion resolves positions and the moment it commits, so the commit validations run
 * against a changed table. The appended rows use their own key range, so they never conflict with
 * the deleted keys and every conversion is expected to succeed.
 */
public class TestIntegrationCdcConcurrentWriter extends CdcIntegrationTestBase {

  private static final TableIdentifier TABLE_IDENTIFIER =
      TableIdentifier.of(TEST_DB, "cdc_concurrent");
  private static final long EXTERNAL_FIRST_ID = 1_000_000L;

  @Test
  public void testConversionSucceedsWhileAnotherWriterAppends() throws Exception {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            TestEvent.TEST_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    startCdcConnector(createConfig(false));

    AtomicBoolean stop = new AtomicBoolean();
    AtomicInteger appended = new AtomicInteger();
    Thread appender =
        new Thread(
            () -> {
              Table table = catalog().loadTable(TABLE_IDENTIFIER);
              while (!stop.get()) {
                appendExternalRow(table, EXTERNAL_FIRST_ID + appended.get());
                appended.incrementAndGet();
                try {
                  Thread.sleep(300);
                } catch (InterruptedException e) {
                  return;
                }
              }
            },
            "external-appender");
    appender.start();

    try {
      sendEvent(event(1, "a", "v1", "I"));
      sendEvent(event(2, "a", "v1", "I"));
      flushEvents();
      awaitCdcRows(appended, expected("1|v1", "2|v1"));

      sendEvent(event(1, "a", "v2", "U"));
      sendEvent(event(2, "a", "v1", "D"));
      sendEvent(event(3, "a", "v1", "I"));
      flushEvents();
      awaitCdcRows(appended, expected("1|v2", "3|v1"));
    } finally {
      stop.set(true);
      appender.join();
    }

    // every external row survived next to the CDC rows
    Set<String> rows = rows(TABLE_IDENTIFIER, "id", "payload");
    assertThat(rows).contains("1|v2", "3|v1").doesNotContain("1|v1", "2|v1");
    for (int i = 0; i < appended.get(); i++) {
      assertThat(rows).contains((EXTERNAL_FIRST_ID + i) + "|external");
    }
    assertThat(appended.get()).isGreaterThan(2);

    assertOnlyDeletionVectors(TABLE_IDENTIFIER);
    assertThat(catalog().loadTable(TABLE_IDENTIFIER).snapshots())
        .noneMatch(snapshot -> snapshot.summary().containsKey("added-equality-delete-files"));
  }

  /** Waits until the CDC rows match, ignoring the rows the external writer adds meanwhile. */
  private void awaitCdcRows(AtomicInteger appended, Set<String> expectedCdcRows) {
    org.awaitility.Awaitility.await()
        .atMost(java.time.Duration.ofSeconds(60))
        .pollInterval(java.time.Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Set<String> cdcRows = Sets.newHashSet();
              for (String row : rows(TABLE_IDENTIFIER, "id", "payload")) {
                if (Long.parseLong(row.substring(0, row.indexOf('|'))) < EXTERNAL_FIRST_ID) {
                  cdcRows.add(row);
                }
              }
              assertThat(cdcRows).isEqualTo(expectedCdcRows);
            });
  }

  private static void appendExternalRow(Table table, long id) {
    Record row = GenericRecord.create(table.schema());
    row.setField("id", id);
    row.setField("type", "external");
    row.setField("ts", OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
    row.setField("payload", "external");

    FileWriterFactory<Record> factory =
        new GenericFileWriterFactory.Builder(table)
            .dataSchema(table.schema())
            .dataFileFormat(FileFormat.PARQUET)
            .build();
    OutputFileFactory outputFiles =
        OutputFileFactory.builderFor(table, 1, System.nanoTime())
            .operationId(UUID.randomUUID().toString())
            .format(FileFormat.PARQUET)
            .build();
    DataWriter<Record> writer =
        factory.newDataWriter(outputFiles.newOutputFile(), table.spec(), null);
    try (DataWriter<Record> closing = writer) {
      closing.write(row);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    table.newAppend().appendFile(writer.toDataFile()).commit();
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    // commits may collide with the external writer at the catalog; let the coordinator retry
    return cdcConfig(TABLE_IDENTIFIER).config("iceberg.control.commit.max-consecutive-failures", 5);
  }

  @Override
  void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }
}
