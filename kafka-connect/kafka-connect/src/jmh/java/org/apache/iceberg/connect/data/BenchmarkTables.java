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
package org.apache.iceberg.connect.data;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tables and writers shared by the equality delete benchmarks. */
final class BenchmarkTables {

  static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()),
              Types.NestedField.optional(3, "payload", Types.StringType.get())),
          ImmutableSet.of(1));

  static final String EQUALITY_DELETES = "EQUALITY_DELETES";
  static final String DELETION_VECTORS = "DELETION_VECTORS";

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTables.class);
  private static final Set<Integer> EQUALITY_FIELD_IDS = ImmutableSet.of(1);
  private static final String PAYLOAD = "x".repeat(64);
  private static final long SEED = 42L;

  private BenchmarkTables() {}

  /** Loads the format version 3 table at the directory, creating it when it does not exist. */
  static Table createOrLoad(File dir) {
    HadoopTables tables = new HadoopTables(new Configuration());
    String location = dir.toURI().toString();
    if (tables.exists(location)) {
      return tables.load(location);
    }

    return tables.create(
        SCHEMA,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"),
        location);
  }

  /** The delta writer the sink uses, without file rolling so one batch yields one data file. */
  static BaseDeltaWriter newWriter(Table table) {
    FileWriterFactory<Record> writerFactory =
        new GenericFileWriterFactory.Builder(table)
            .dataSchema(SCHEMA)
            .dataFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {1})
            .equalityDeleteRowSchema(TypeUtil.select(SCHEMA, EQUALITY_FIELD_IDS))
            .deleteFileFormat(FileFormat.PARQUET)
            .build();
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.nanoTime())
            .format(FileFormat.PARQUET)
            .build();
    return new UnpartitionedDeltaWriter(
        table.spec(),
        FileFormat.PARQUET,
        writerFactory,
        fileFactory,
        table.io(),
        Long.MAX_VALUE,
        SCHEMA,
        EQUALITY_FIELD_IDS,
        false,
        true);
  }

  static Record row(long id, String payload) {
    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", id);
    row.setField("category", "c" + (id % 10));
    row.setField("payload", payload == null ? PAYLOAD : payload);
    return row;
  }

  /** Writes one data file holding the ids [first, first + count). */
  static DataFile writeDataFile(Table table, long first, long count) {
    BaseDeltaWriter writer = newWriter(table);
    try {
      for (long id = first; id < first + count; id++) {
        writer.write(row(id, null), Operation.INSERT);
      }

      WriteResult result = writer.complete();
      return result.dataFiles()[0];
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Writes the equality delete file(s) for the given keys and returns them uncommitted. */
  static List<DeleteFile> writeEqualityDeletes(Table table, Iterable<Long> ids) {
    BaseDeltaWriter writer = newWriter(table);
    try {
      for (long id : ids) {
        writer.write(row(id, null), Operation.DELETE);
      }

      return Arrays.asList(writer.complete().deleteFiles());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Rows per CDC commit, inserted and updated alike. */
  static final int ROWS_PER_COMMIT = 500;

  /**
   * Loads the CDC table with the given number of commits, building it when it does not exist or an
   * earlier run stopped half way. Every commit inserts {@link #ROWS_PER_COMMIT} new keys and
   * updates as many random keys of earlier commits. The deletes are committed as equality delete
   * files or converted into deletion vectors, depending on {@code deleteMode}.
   */
  static Table cdcTable(File dir, int commits, String deleteMode) {
    Table table = createOrLoad(dir);
    int existing = Iterables.size(SnapshotUtil.currentAncestors(table));
    if (existing == commits) {
      return table;
    } else if (existing > 0) {
      LOG.info("Table {} has {} of {} commits, rebuilding it", dir, existing, commits);
      deleteRecursively(dir);
      table = createOrLoad(dir);
    }

    LOG.info("Writing {} commits in {} mode to {}", commits, deleteMode, dir);
    Random random = new Random(SEED);
    long writeMillis = 0;
    long commitMillis = 0;
    for (int commit = 0; commit < commits; commit++) {
      long start = System.currentTimeMillis();
      long firstNew = (long) commit * ROWS_PER_COMMIT;
      WriteResult result =
          writeBatch(
              table,
              firstNew,
              commit == 0 ? 0 : ROWS_PER_COMMIT,
              () -> (long) (random.nextDouble() * firstNew),
              "updated in commit " + commit);
      long written = System.currentTimeMillis();
      commitBatch(table, result, deleteMode);
      long committed = System.currentTimeMillis();
      writeMillis += written - start;
      commitMillis += committed - written;
      if ((commit + 1) % 50 == 0) {
        LOG.info(
            "{} commits: {} ms writing, {} ms committing in the last 50",
            commit + 1,
            writeMillis,
            commitMillis);
        writeMillis = 0;
        commitMillis = 0;
      }
    }

    return table;
  }

  /**
   * Writes one CDC batch: {@link #ROWS_PER_COMMIT} inserts of the ids starting at {@code firstNew}
   * and {@code updates} updates of the ids the supplier returns.
   */
  static WriteResult writeBatch(
      Table table, long firstNew, int updates, LongSupplier updatedIds, String updatePayload) {
    BaseDeltaWriter writer = newWriter(table);
    try {
      for (long id = firstNew; id < firstNew + ROWS_PER_COMMIT; id++) {
        writer.write(row(id, null), Operation.INSERT);
      }

      for (int i = 0; i < updates; i++) {
        writer.write(row(updatedIds.getAsLong(), updatePayload), Operation.UPDATE);
      }

      return writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Commits a batch the way the sink's coordinator does. The writer removes duplicates within the
   * batch with deletion vectors; the equality deletes for rows of earlier commits are committed as
   * they are or converted first, depending on {@code deleteMode}.
   *
   * @return the conversion result, or null when nothing was converted
   */
  static EqualityDeleteConverter.Result commitBatch(
      Table table, WriteResult result, String deleteMode) {
    List<DataFile> dataFiles = ImmutableList.copyOf(result.dataFiles());
    List<DeleteFile> eqDeleteFiles = Lists.newArrayList();
    List<DeleteFile> otherDeleteFiles = Lists.newArrayList();
    for (DeleteFile deleteFile : result.deleteFiles()) {
      if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
        eqDeleteFiles.add(deleteFile);
      } else {
        otherDeleteFiles.add(deleteFile);
      }
    }

    Long base = table.currentSnapshot() == null ? null : table.currentSnapshot().snapshotId();

    if (deleteMode.equals(EQUALITY_DELETES) || eqDeleteFiles.isEmpty()) {
      commit(
          table,
          dataFiles,
          Iterables.concat(otherDeleteFiles, eqDeleteFiles),
          ImmutableList.of(),
          base);
      return null;
    }

    EqualityDeleteConverter.Result converted =
        new EqualityDeleteConverter(table, null).convert(eqDeleteFiles);
    commit(
        table,
        dataFiles,
        Iterables.concat(otherDeleteFiles, converted.dvFiles()),
        converted.rewrittenDvFiles(),
        base);
    for (DeleteFile deleteFile : eqDeleteFiles) {
      table.io().deleteFile(deleteFile.location());
    }

    return converted;
  }

  static void commit(
      Table table,
      Iterable<DataFile> dataFiles,
      Iterable<DeleteFile> addedDeletes,
      Iterable<DeleteFile> removedDeletes,
      Long baseSnapshotId) {
    RowDelta rowDelta = table.newRowDelta();
    if (baseSnapshotId != null) {
      rowDelta.validateFromSnapshot(baseSnapshotId);
    }
    dataFiles.forEach(rowDelta::addRows);
    addedDeletes.forEach(rowDelta::addDeletes);
    removedDeletes.forEach(rowDelta::removeDeletes);
    rowDelta.commit();
  }

  static void deleteRecursively(File file) {
    File[] children = file.listFiles();
    if (children != null) {
      for (File child : children) {
        deleteRecursively(child);
      }
    }

    if (!file.delete() && file.exists()) {
      throw new UncheckedIOException(new IOException("Cannot delete " + file));
    }
  }
}
