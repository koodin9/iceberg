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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves equality delete files against the rows of a table and writes the matching row positions
 * as deletion vectors, so the equality delete files never have to be committed.
 *
 * <p>An equality delete written by the sink deletes every row of an earlier commit whose identifier
 * fields equal one of the deleted keys. To find those rows without an index, the converter plans
 * the data files of the current snapshot with a filter built from the deleted keys (an {@code IN}
 * list for a small key set, a value range per key column for a large one), reads only the key
 * columns and the row position of the remaining candidate files and keeps the positions whose key
 * is in the deleted set. A data file that already has a deletion vector gets a merged one; the
 * replaced vector is reported in {@link Result#rewrittenDvFiles()} and must be removed by the
 * commit.
 *
 * <p>The positions are only valid for the snapshot they were resolved against, see {@link
 * Result#baseSnapshotId()}. The commit that adds the deletion vectors has to validate that no
 * conflicting data or delete files were added since then and re-run the conversion otherwise.
 *
 * <p>Requires format version 3. Data files that still carry position delete files instead of
 * deletion vectors are rejected, because a deletion vector replaces all position deletes of its
 * data file.
 */
public class EqualityDeleteConverter {

  private static final Logger LOG = LoggerFactory.getLogger(EqualityDeleteConverter.class);

  // metrics evaluators stop pruning with IN predicates above this many values, a range is used then
  private static final int IN_PREDICATE_LIMIT = 200;

  private final Table table;
  private final String branch;
  private final DeleteLoader deleteLoader;
  private final AtomicInteger scannedDataFiles = new AtomicInteger();

  /**
   * @param table the table to resolve the deletes against
   * @param branch the branch that is written, or null for the main branch
   */
  public EqualityDeleteConverter(Table table, String branch) {
    this.table = table;
    this.branch = branch;
    this.deleteLoader = new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
  }

  /**
   * Converts the given equality delete files into deletion vectors on the current snapshot of the
   * branch. The table is expected to be refreshed by the caller.
   */
  public Result convert(List<DeleteFile> eqDeleteFiles) {
    Snapshot base = branch == null ? table.currentSnapshot() : table.snapshot(branch);
    if (base == null) {
      // there are no rows an equality delete could remove
      LOG.info(
          "Table {} has no snapshot on branch {}, {} equality delete file(s) match no rows",
          table.name(),
          branch == null ? "main" : branch,
          eqDeleteFiles.size());
      return Result.empty();
    }

    Map<Set<Integer>, List<DeleteFile>> filesByEqualityFields = Maps.newLinkedHashMap();
    for (DeleteFile deleteFile : eqDeleteFiles) {
      Preconditions.checkArgument(
          deleteFile.content() == FileContent.EQUALITY_DELETES,
          "Not an equality delete file: %s",
          deleteFile.location());
      filesByEqualityFields
          .computeIfAbsent(
              ImmutableSet.copyOf(deleteFile.equalityFieldIds()), ids -> Lists.newArrayList())
          .add(deleteFile);
    }

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(FileFormat.PUFFIN)
            .build();
    // deletion vectors already attached to a matched data file, folded into the new vector
    Map<String, PositionDeleteIndex> previousDeletes = Maps.newConcurrentMap();
    BaseDVFileWriter dvWriter = new BaseDVFileWriter(fileFactory, previousDeletes::get);

    Expression conflictFilter = Expressions.alwaysFalse();
    long matchedRows = 0L;
    long deletedKeys = 0L;
    scannedDataFiles.set(0);

    long start = System.currentTimeMillis();
    try {
      for (Map.Entry<Set<Integer>, List<DeleteFile>> entry : filesByEqualityFields.entrySet()) {
        Schema keySchema = keySchema(entry.getKey());
        StructLikeSet keys = deleteLoader.loadEqualityDeletes(entry.getValue(), keySchema);
        if (keys.isEmpty()) {
          continue;
        }

        deletedKeys += keys.size();
        Expression filter = keyFilter(keySchema, keys);
        conflictFilter = Expressions.or(conflictFilter, filter);
        matchedRows += resolve(base, keySchema, keys, filter, previousDeletes, dvWriter);
      }

      dvWriter.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    DeleteWriteResult writeResult = dvWriter.result();
    LOG.info(
        "Converted {} equality delete file(s) with {} key(s) into {} deletion vector(s) covering {} row(s) "
            + "of table {} at snapshot {}, scanned {} data file(s) in {} ms",
        eqDeleteFiles.size(),
        deletedKeys,
        writeResult.deleteFiles().size(),
        matchedRows,
        table.name(),
        base.snapshotId(),
        scannedDataFiles.get(),
        System.currentTimeMillis() - start);

    return new Result(
        base.snapshotId(),
        writeResult.deleteFiles(),
        writeResult.rewrittenDeleteFiles(),
        conflictFilter);
  }

  @VisibleForTesting
  int scannedDataFiles() {
    return scannedDataFiles.get();
  }

  private Schema keySchema(Set<Integer> equalityFieldIds) {
    Schema keySchema = TypeUtil.select(table.schema(), equalityFieldIds);
    Preconditions.checkArgument(
        TypeUtil.getProjectedIds(keySchema).containsAll(equalityFieldIds),
        "Equality field IDs %s are not all present in table schema %s",
        equalityFieldIds,
        table.schema());
    return keySchema;
  }

  /**
   * Plans the data files of the base snapshot that may hold a deleted key, reads them in parallel
   * and adds the matching positions to the deletion vector writer. Returns the number of matched
   * rows.
   */
  private long resolve(
      Snapshot base,
      Schema keySchema,
      StructLikeSet keys,
      Expression filter,
      Map<String, PositionDeleteIndex> previousDeletes,
      BaseDVFileWriter dvWriter)
      throws IOException {
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> planned =
        table
            .newScan()
            .useSnapshot(base.snapshotId())
            .filter(filter)
            .ignoreResiduals()
            .planFiles()) {
      tasks = Lists.newArrayList(planned);
    }

    scannedDataFiles.addAndGet(tasks.size());

    Map<String, FileMatches> matches = Maps.newConcurrentMap();
    Tasks.foreach(tasks)
        .executeWith(ThreadPools.getWorkerPool())
        .stopOnFailure()
        .throwFailureWhenFinished()
        .run(
            task -> match(task, keySchema, keys, filter, previousDeletes, matches),
            IOException.class);

    AtomicLong matchedRows = new AtomicLong();
    for (Map.Entry<String, FileMatches> entry : matches.entrySet()) {
      String path = entry.getKey();
      FileMatches fileMatches = entry.getValue();
      fileMatches
          .positions()
          .forEach(
              pos -> {
                dvWriter.delete(path, pos, fileMatches.spec(), fileMatches.partition());
                matchedRows.incrementAndGet();
              });
    }

    return matchedRows.get();
  }

  /** Reads the key columns and row positions of one data file and records the matching rows. */
  private void match(
      FileScanTask task,
      Schema keySchema,
      StructLikeSet keys,
      Expression filter,
      Map<String, PositionDeleteIndex> previousDeletes,
      Map<String, FileMatches> matches)
      throws IOException {
    DataFile file = task.file();

    List<DeleteFile> dvs = Lists.newArrayList();
    for (DeleteFile delete : task.deletes()) {
      if (ContentFileUtil.isDV(delete)) {
        dvs.add(delete);
      } else if (delete.content() == FileContent.POSITION_DELETES) {
        throw new IllegalStateException(
            String.format(
                "Data file %s has position delete file %s, rewrite position deletes into deletion "
                    + "vectors before enabling equality delete conversion",
                file.location(), delete.location()));
      }
      // an attached equality delete stays in the table and is still applied by readers
    }

    PositionDeleteIndex existing = null;
    if (!dvs.isEmpty()) {
      existing = deleteLoader.loadPositionDeletes(dvs, file.location());
      previousDeletes.put(file.location(), existing);
    }

    Schema readSchema =
        new Schema(
            ImmutableList.<Types.NestedField>builder()
                .addAll(keySchema.columns())
                .add(MetadataColumns.ROW_POSITION)
                .build());
    Accessor<StructLike> posAccessor =
        readSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
    StructProjection keyProjection = StructProjection.create(readSchema, keySchema);
    InternalRecordWrapper keyWrapper = new InternalRecordWrapper(keySchema.asStruct());

    InputFile input = table.io().newInputFile(file);
    ReadBuilder<Record, Schema> reader =
        FormatModelRegistry.readBuilder(file.format(), Record.class, input);
    Positions positions = new Positions();
    try (CloseableIterable<Record> records =
        reader.project(readSchema).filter(filter).reuseContainers().build()) {
      for (Record record : records) {
        long pos = (long) posAccessor.get(record);
        if (existing != null && existing.isDeleted(pos)) {
          continue;
        }

        if (keys.contains(keyWrapper.wrap(keyProjection.wrap(record)))) {
          positions.add(pos);
        }
      }
    }

    if (!positions.isEmpty()) {
      matches.put(file.location(), new FileMatches(task.spec(), file.partition(), positions));
    }
  }

  /**
   * Builds a file pruning filter from the deleted keys. Rows are still matched exactly, so the
   * filter only has to be inclusive: an {@code IN} list per key column while the metrics evaluators
   * still use it for pruning, a value range per key column above that.
   */
  @SuppressWarnings("unchecked")
  private static Expression keyFilter(Schema keySchema, StructLikeSet keys) {
    Expression filter = Expressions.alwaysTrue();
    List<Types.NestedField> columns = keySchema.columns();
    for (int pos = 0; pos < columns.size(); pos++) {
      Types.NestedField column = columns.get(pos);
      if (!column.type().isPrimitiveType()) {
        // no pruning on a nested key column
        continue;
      }

      Set<Object> values = Sets.newHashSet();
      boolean hasNull = false;
      for (StructLike key : keys) {
        Object value = key.get(pos, Object.class);
        if (value == null) {
          hasNull = true;
        } else {
          values.add(value);
        }
      }

      Expression columnFilter;
      if (values.isEmpty()) {
        columnFilter = Expressions.alwaysFalse();
      } else if (values.size() <= IN_PREDICATE_LIMIT) {
        columnFilter = Expressions.in(column.name(), values);
      } else {
        Comparator<Object> comparator =
            (Comparator<Object>) Comparators.forType(column.type().asPrimitiveType());
        Object min = Collections.min(values, comparator);
        Object max = Collections.max(values, comparator);
        columnFilter =
            Expressions.and(
                Expressions.greaterThanOrEqual(column.name(), min),
                Expressions.lessThanOrEqual(column.name(), max));
      }

      if (hasNull) {
        columnFilter = Expressions.or(columnFilter, Expressions.isNull(column.name()));
      }

      filter = Expressions.and(filter, columnFilter);
    }

    return filter;
  }

  /** Outcome of a conversion, to be committed together with the data files of the batch. */
  public static class Result {
    private static final Result EMPTY =
        new Result(null, ImmutableList.of(), ImmutableList.of(), Expressions.alwaysFalse());

    private final Long baseSnapshotId;
    private final List<DeleteFile> dvFiles;
    private final List<DeleteFile> rewrittenDvFiles;
    private final Expression conflictFilter;

    Result(
        Long baseSnapshotId,
        List<DeleteFile> dvFiles,
        List<DeleteFile> rewrittenDvFiles,
        Expression conflictFilter) {
      this.baseSnapshotId = baseSnapshotId;
      this.dvFiles = ImmutableList.copyOf(dvFiles);
      this.rewrittenDvFiles = ImmutableList.copyOf(rewrittenDvFiles);
      this.conflictFilter = conflictFilter;
    }

    static Result empty() {
      return EMPTY;
    }

    /** Snapshot the positions were resolved against, or null when the branch had no snapshot. */
    public Long baseSnapshotId() {
      return baseSnapshotId;
    }

    /** Deletion vectors to add. */
    public List<DeleteFile> dvFiles() {
      return dvFiles;
    }

    /** Deletion vectors that were merged into a new one and must be removed. */
    public List<DeleteFile> rewrittenDvFiles() {
      return rewrittenDvFiles;
    }

    /**
     * Row filter covering every deleted key, for detecting data or delete files that were added
     * concurrently for those keys.
     */
    public Expression conflictFilter() {
      return conflictFilter;
    }

    /** Data files referenced by the new deletion vectors. */
    public Set<CharSequence> referencedDataFiles() {
      Set<CharSequence> referenced = Sets.newHashSet();
      for (DeleteFile dvFile : dvFiles) {
        referenced.add(dvFile.referencedDataFile());
      }

      return referenced;
    }
  }

  private static class FileMatches {
    private final PartitionSpec spec;
    private final StructLike partition;
    private final Positions positions;

    FileMatches(PartitionSpec spec, StructLike partition, Positions positions) {
      this.spec = spec;
      this.partition = partition;
      this.positions = positions;
    }

    PartitionSpec spec() {
      return spec;
    }

    StructLike partition() {
      return partition;
    }

    Positions positions() {
      return positions;
    }
  }

  /** Growable list of row positions without boxing. */
  private static class Positions {
    private long[] values = new long[64];
    private int size = 0;

    void add(long value) {
      if (size == values.length) {
        long[] grown = new long[values.length * 2];
        System.arraycopy(values, 0, grown, 0, size);
        this.values = grown;
      }

      values[size] = value;
      size++;
    }

    boolean isEmpty() {
      return size == 0;
    }

    void forEach(LongConsumer consumer) {
      for (int i = 0; i < size; i++) {
        consumer.accept(values[i]);
      }
    }
  }
}
