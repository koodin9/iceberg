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
package org.apache.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.IcebergWriterResult;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives CDC records through the sink writer and the coordinator, and checks that equality deletes
 * reach the table as deletion vectors.
 */
public class TestCoordinatorConvertEqualityDeletes extends ChannelTestBase {

  private static final String CDC_TABLE_NAME = "db.cdc";
  private static final TableIdentifier CDC_TABLE_IDENTIFIER = TableIdentifier.parse(CDC_TABLE_NAME);
  private static final String CONVERTED_EQ_DELETE_FILES_SNAPSHOT_PROP =
      "kafka.connect.converted-equality-delete-files";

  private static final org.apache.iceberg.Schema CDC_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));

  private static final Schema CDC_META_SCHEMA =
      SchemaBuilder.struct().field("op", Schema.STRING_SCHEMA).build();
  private static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .field("id", Schema.INT64_SCHEMA)
          .field("data", Schema.OPTIONAL_STRING_SCHEMA)
          .field("_cdc", CDC_META_SCHEMA)
          .build();

  private Table cdcTable;
  private long sourceOffset = 0;
  private long controlOffset = 1;

  @BeforeEach
  public void beforeConversion() {
    cdcTable =
        catalog.createTable(
            CDC_TABLE_IDENTIFIER,
            CDC_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", "3"));

    when(config.tables()).thenReturn(ImmutableList.of(CDC_TABLE_NAME));
    when(config.writeProps()).thenReturn(ImmutableMap.of());
    when(config.tablesCdcField()).thenReturn("_cdc.op");
    when(config.convertEqualityDeletesEnabled()).thenReturn(true);
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
  }

  @Test
  public void testEqualityDeletesCommittedAsDeletionVectors() {
    Coordinator coordinator = startCoordinator();

    List<IcebergWriterResult> first = write(record(1L, "v1", "I"), record(2L, "v1", "I"));
    commitCycle(coordinator, first);

    cdcTable.refresh();
    Snapshot firstSnapshot = cdcTable.currentSnapshot();
    assertThat(changes(firstSnapshot).addedDataFiles()).hasSize(1);
    assertThat(changes(firstSnapshot).addedDeleteFiles()).isEmpty();
    assertThat(firstSnapshot.summary()).doesNotContainKey(CONVERTED_EQ_DELETE_FILES_SNAPSHOT_PROP);

    List<IcebergWriterResult> second = write(record(1L, "v2", "U"), record(2L, null, "D"));
    List<DeleteFile> eqDeleteFiles = deleteFiles(second, FileContent.EQUALITY_DELETES);
    assertThat(eqDeleteFiles).hasSize(1);
    commitCycle(coordinator, second);

    cdcTable.refresh();
    Snapshot secondSnapshot = cdcTable.currentSnapshot();
    assertThat(secondSnapshot.snapshotId()).isNotEqualTo(firstSnapshot.snapshotId());
    assertThat(changes(secondSnapshot).addedDataFiles()).hasSize(1);
    List<DeleteFile> addedDeletes = Lists.newArrayList(changes(secondSnapshot).addedDeleteFiles());
    assertThat(addedDeletes).hasSize(1);
    assertThat(addedDeletes).allMatch(ContentFileUtil::isDV);
    assertThat(addedDeletes.get(0).recordCount()).isEqualTo(2L);
    assertThat(secondSnapshot.summary())
        .containsEntry(CONVERTED_EQ_DELETE_FILES_SNAPSHOT_PROP, "1")
        .containsEntry(
            COMMIT_ID_SNAPSHOT_PROP, secondSnapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP))
        .containsKey(OFFSETS_SNAPSHOT_PROP);

    // the equality delete file is not referenced by any snapshot and was removed
    assertThat(((InMemoryFileIO) cdcTable.io()).fileExists(eqDeleteFiles.get(0).location()))
        .isFalse();
    assertThat(readRows(cdcTable)).containsExactly("1|v2");
    assertThat(coordinator.eqDeleteConversionFallbackCount()).isEqualTo(0);
  }

  @Test
  public void testFallsBackToEqualityDeletesAfterRepeatedConflicts() {
    Table spiedTable = spy(cdcTable);
    AtomicInteger rowDeltas = new AtomicInteger();
    when(spiedTable.newRowDelta())
        .thenAnswer(
            invocation -> {
              RowDelta rowDelta = (RowDelta) invocation.callRealMethod();
              // every attempt to commit converted deletion vectors sees a conflict
              if (rowDeltas.getAndIncrement() < 3) {
                RowDelta conflicting = spy(rowDelta);
                doThrow(new ValidationException("simulated conflict")).when(conflicting).commit();
                return conflicting;
              }

              return rowDelta;
            });
    when(catalog.loadTable(CDC_TABLE_IDENTIFIER)).thenReturn(spiedTable);

    Coordinator coordinator = startCoordinator();
    commitCycle(coordinator, write(record(1L, "v1", "I"), record(2L, "v1", "I")));

    // the first commit has no equality deletes and does not use a RowDelta
    assertThat(rowDeltas.get()).isEqualTo(0);

    commitCycle(coordinator, write(record(1L, "v2", "U"), record(2L, null, "D")));

    // three failed conversion attempts, then the fallback commit
    assertThat(rowDeltas.get()).isEqualTo(4);
    assertThat(coordinator.eqDeleteConversionFallbackCount()).isEqualTo(1);

    cdcTable.refresh();
    Snapshot snapshot = cdcTable.currentSnapshot();
    List<DeleteFile> addedDeletes = Lists.newArrayList(changes(snapshot).addedDeleteFiles());
    assertThat(addedDeletes).hasSize(1);
    assertThat(addedDeletes.get(0).content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(snapshot.summary()).doesNotContainKey(CONVERTED_EQ_DELETE_FILES_SNAPSHOT_PROP);
    assertThat(readRows(cdcTable)).containsExactly("1|v2");
  }

  private Coordinator startCoordinator() {
    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
    coordinator.start();
    initConsumer();
    return coordinator;
  }

  private List<IcebergWriterResult> write(SinkRecord... records) {
    SinkWriter sinkWriter = new SinkWriter(catalog, config);
    sinkWriter.save(ImmutableList.copyOf(records));
    SinkWriterResult result = sinkWriter.completeWrite();
    sinkWriter.close();
    return result.writerResults();
  }

  /** Runs one commit cycle: start commit, worker responses, commit to the table. */
  private void commitCycle(Coordinator coordinator, List<IcebergWriterResult> writerResults) {
    coordinator.process();

    byte[] startBytes = producer.history().get(producer.history().size() - 1).value();
    UUID commitId = ((StartCommit) AvroUtil.decode(startBytes).payload()).commitId();

    for (IcebergWriterResult writerResult : writerResults) {
      Event dataWritten =
          new Event(
              config.connectGroupId(),
              new DataWritten(
                  writerResult.partitionStruct(),
                  commitId,
                  writerResult.tableReference(),
                  writerResult.dataFiles(),
                  writerResult.deleteFiles()));
      consumer.addRecord(
          new ConsumerRecord<>(
              CTL_TOPIC_NAME, 0, controlOffset++, "key", AvroUtil.encode(dataWritten)));
    }

    Event dataComplete =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId,
                ImmutableList.of(
                    new TopicPartitionOffset(
                        SRC_TOPIC_NAME, 0, sourceOffset, EventTestUtil.now()))));
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME, 0, controlOffset++, "key", AvroUtil.encode(dataComplete)));

    coordinator.process();
  }

  private SinkRecord record(long id, String data, String op) {
    Struct cdc = new Struct(CDC_META_SCHEMA).put("op", op);
    Struct value = new Struct(VALUE_SCHEMA).put("id", id).put("data", data).put("_cdc", cdc);
    return new SinkRecord(SRC_TOPIC_NAME, 0, null, null, VALUE_SCHEMA, value, sourceOffset++);
  }

  private SnapshotChanges changes(Snapshot snapshot) {
    return SnapshotChanges.builderFor(cdcTable).snapshot(snapshot).build();
  }

  private static List<DeleteFile> deleteFiles(
      List<IcebergWriterResult> writerResults, FileContent content) {
    List<DeleteFile> files = Lists.newArrayList();
    for (IcebergWriterResult writerResult : writerResults) {
      for (DeleteFile deleteFile : writerResult.deleteFiles()) {
        if (deleteFile.content() == content) {
          files.add(deleteFile);
        }
      }
    }
    return files;
  }

  private static Set<String> readRows(Table table) {
    Set<String> rows = Sets.newHashSet();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(record.getField("id") + "|" + record.getField("data"));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }
}
