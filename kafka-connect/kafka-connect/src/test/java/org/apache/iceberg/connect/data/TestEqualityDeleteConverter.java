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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.LongConsumer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestEqualityDeleteConverter {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("test_catalog", TABLE_IDENTIFIER, UUID.randomUUID());

  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()),
              Types.NestedField.optional(3, "data", Types.StringType.get())),
          ImmutableSet.of(1));

  private InMemoryCatalog catalog;
  private IcebergSinkConfig config;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test_catalog", ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);

    config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of());
    when(config.tablesCdcField()).thenReturn("_cdc.op");
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testConvertsDeletesOfEarlierCommits() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult first =
        write(
            table,
            insert(1L, "a", "v1"),
            insert(2L, "a", "v1"),
            insert(3L, "a", "v1"),
            insert(4L, "a", "v1"),
            insert(5L, "a", "v1"));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());
    DataFile firstDataFile = first.dataFiles()[0];
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    WriteResult second = write(table, delete(2L, "a"), update(4L, "a", "v2"));
    assertThat(second.deleteFiles()).hasSize(1);

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    assertThat(result.baseSnapshotId()).isEqualTo(firstSnapshotId);
    assertThat(result.rewrittenDvFiles()).isEmpty();
    assertThat(result.dvFiles()).hasSize(1);
    DeleteFile dv = result.dvFiles().get(0);
    assertThat(ContentFileUtil.isDV(dv)).isTrue();
    assertThat(dv.referencedDataFile()).isEqualTo(firstDataFile.location());
    assertThat(dv.recordCount()).isEqualTo(2L);
    assertThat(result.referencedDataFiles()).containsExactly(firstDataFile.location());
    assertThat(converter.scannedDataFiles()).isEqualTo(1);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).containsExactlyInAnyOrder("1|a|v1", "3|a|v1", "4|a|v2", "5|a|v1");

    // a second conversion for the same data file merges the existing deletion vector
    WriteResult third = write(table, delete(1L, "a"));
    result = converter.convert(Arrays.asList(third.deleteFiles()));

    assertThat(result.dvFiles()).hasSize(1);
    assertThat(result.dvFiles().get(0).referencedDataFile()).isEqualTo(firstDataFile.location());
    assertThat(result.dvFiles().get(0).recordCount()).isEqualTo(3L);
    assertThat(result.rewrittenDvFiles()).hasSize(1);
    assertThat(result.rewrittenDvFiles().get(0).location()).isEqualTo(dv.location());
    assertThat(result.rewrittenDvFiles().get(0).contentOffset()).isEqualTo(dv.contentOffset());
    // the data file written by the second batch only holds id 4 and is pruned by its column bounds
    assertThat(converter.scannedDataFiles()).isEqualTo(1);

    commit(table, third.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).containsExactlyInAnyOrder("3|a|v1", "4|a|v2", "5|a|v1");
  }

  @Test
  public void testPartitionPruning() throws IOException {
    Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("category").build());
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.idColumns()).thenReturn(ImmutableList.of("id", "category"));
    when(config.tableConfig(any())).thenReturn(tableConfig);

    WriteResult first =
        write(
            table,
            insert(1L, "a", "v1"),
            insert(2L, "a", "v1"),
            insert(3L, "b", "v1"),
            insert(4L, "b", "v1"));
    assertThat(first.dataFiles()).hasSize(2);
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());

    WriteResult second = write(table, delete(1L, "a"));
    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    // only the data file of partition a is a candidate
    assertThat(converter.scannedDataFiles()).isEqualTo(1);
    assertThat(result.dvFiles()).hasSize(1);
    assertThat(result.dvFiles().get(0).recordCount()).isEqualTo(1L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).containsExactlyInAnyOrder("2|a|v1", "3|b|v1", "4|b|v1");
  }

  @Test
  public void testLargeKeySetUsesRangeFilter() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());

    List<Object[]> inserts = Lists.newArrayList();
    for (long id = 1; id <= 300; id++) {
      inserts.add(insert(id, "a", "v1"));
    }
    WriteResult first = write(table, inserts.toArray(new Object[0][]));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());

    List<Object[]> deletes = Lists.newArrayList();
    for (long id = 1; id <= 250; id++) {
      deletes.add(delete(id, "a"));
    }
    WriteResult second = write(table, deletes.toArray(new Object[0][]));

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    assertThat(result.dvFiles()).hasSize(1);
    assertThat(result.dvFiles().get(0).recordCount()).isEqualTo(250L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    Set<String> rows = readRows(table);
    assertThat(rows).hasSize(50);
    assertThat(rows).contains("251|a|v1", "300|a|v1");
    assertThat(rows).doesNotContain("1|a|v1", "250|a|v1");
  }

  @Test
  public void testClusteredKeysPruneToTheirFiles() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());

    // 200 data files with 50 consecutive ids each
    int fileCount = 200;
    int rowsPerFile = 50;
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int file = 0; file < fileCount; file++) {
      List<Object[]> inserts = Lists.newArrayList();
      for (int row = 0; row < rowsPerFile; row++) {
        inserts.add(insert((long) file * rowsPerFile + row, "a", "v1"));
      }
      WriteResult result = write(table, inserts.toArray(new Object[0][]));
      assertThat(result.dataFiles()).hasSize(1);
      dataFiles.add(result.dataFiles()[0]);
    }
    commit(table, dataFiles.toArray(new DataFile[0]), new DeleteFile[0], ImmutableList.of());

    // 295 hot keys in the last six files and five keys scattered over the table
    List<Object[]> deletes = Lists.newArrayList();
    for (long id = 9700; id < 9995; id++) {
      deletes.add(delete(id, "a"));
    }
    for (long id : new long[] {5, 1005, 3005, 6005, 8005}) {
      deletes.add(delete(id, "a"));
    }
    WriteResult second = write(table, deletes.toArray(new Object[0][]));

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    // more keys than the IN limit, so the keys are split into ranges at the largest gaps: the hot
    // range covers six files and every scattered key stays in its own file
    assertThat(deletes).hasSizeGreaterThan(KeyFilters.IN_PREDICATE_LIMIT);
    assertThat(converter.scannedDataFiles()).isEqualTo(11);
    assertThat(result.dvFiles()).hasSize(11);
    assertThat(result.dvFiles().stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(300L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).hasSize(fileCount * rowsPerFile - 300);
  }

  @Test
  public void testRangeFilterIsScopedToPartition() throws IOException {
    Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("category").build());
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.idColumns()).thenReturn(ImmutableList.of("id", "category"));
    when(config.tableConfig(any())).thenReturn(tableConfig);

    // three files of 100 ids in each of the partitions a and b, ids overlap across partitions
    List<DataFile> dataFiles = Lists.newArrayList();
    for (String category : new String[] {"a", "b"}) {
      for (int file = 0; file < 3; file++) {
        List<Object[]> inserts = Lists.newArrayList();
        for (int row = 0; row < 100; row++) {
          inserts.add(insert((long) file * 100 + row, category, "v1"));
        }
        WriteResult result = write(table, inserts.toArray(new Object[0][]));
        dataFiles.add(result.dataFiles()[0]);
      }
    }
    commit(table, dataFiles.toArray(new DataFile[0]), new DeleteFile[0], ImmutableList.of());

    List<Object[]> deletes = Lists.newArrayList();
    for (long id = 0; id < 250; id++) {
      deletes.add(delete(id, "a"));
    }
    WriteResult second = write(table, deletes.toArray(new Object[0][]));

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    // the range on id covers every file of both partitions, the partition pins it to a
    assertThat(converter.scannedDataFiles()).isEqualTo(3);
    assertThat(result.dvFiles()).hasSize(3);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    Set<String> rows = readRows(table);
    assertThat(rows).hasSize(350);
    assertThat(rows).contains("0|b|v1", "250|a|v1").doesNotContain("0|a|v1", "249|a|v1");
  }

  @Test
  public void testTransformPartitionIsPinnedInRangeMode() throws IOException {
    Table table = createTable(PartitionSpec.builderFor(SCHEMA).bucket("id", 4).build());

    // three batches, each writes one file per bucket
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int batch = 0; batch < 3; batch++) {
      List<Object[]> inserts = Lists.newArrayList();
      for (long id = batch * 1000L; id < (batch + 1) * 1000L; id++) {
        inserts.add(insert(id, "a", "v1"));
      }
      WriteResult result = write(table, inserts.toArray(new Object[0][]));
      assertThat(result.dataFiles()).hasSize(4);
      dataFiles.addAll(Arrays.asList(result.dataFiles()));
    }
    commit(table, dataFiles.toArray(new DataFile[0]), new DeleteFile[0], ImmutableList.of());

    // more keys than the IN limit, all of them in bucket 0 and spread over all three batches
    Function<Object, Integer> bucket = Transforms.bucket(4).bind(Types.LongType.get());
    List<Long> bucketZeroIds = Lists.newArrayList();
    for (long id = 0; id < 3000; id++) {
      if (bucket.apply(id) == 0) {
        bucketZeroIds.add(id);
      }
    }
    List<Object[]> deletes = Lists.newArrayList();
    for (int i = 0; i < 250; i++) {
      deletes.add(delete(bucketZeroIds.get(i * bucketZeroIds.size() / 250), "a"));
    }
    WriteResult second = write(table, deletes.toArray(new Object[0][]));
    assertThat(second.deleteFiles()).hasSize(1);

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    // the id range covers every file, the pinned bucket keeps only the three files of bucket 0
    assertThat(converter.scannedDataFiles()).isEqualTo(3);
    assertThat(result.dvFiles()).hasSize(3);
    assertThat(result.dvFiles().stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(250L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).hasSize(3000 - 250);
  }

  @Test
  public void testCustomResolver() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult first = write(table, insert(1L, "a", "v1"), insert(2L, "a", "v1"));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());
    DataFile dataFile = first.dataFiles()[0];

    WriteResult second = write(table, delete(2L, "a"));

    // a resolver that knows the position of the row without reading the data file
    KeyPositionResolver resolver =
        (base, keySchema, keys, deleteSpec, deletePartition) -> {
          assertThat(keys).hasSize(1);
          KeyPositionResolver.FileMatches matches =
              new KeyPositionResolver.FileMatches() {
                @Override
                public String path() {
                  return dataFile.location();
                }

                @Override
                public PartitionSpec spec() {
                  return table.spec();
                }

                @Override
                public StructLike partition() {
                  return dataFile.partition();
                }

                @Override
                public PositionDeleteIndex existingDeletes() {
                  return null;
                }

                @Override
                public void forEachPosition(LongConsumer consumer) {
                  consumer.accept(1L);
                }
              };
          return new KeyPositionResolver.Resolution() {
            @Override
            public Collection<KeyPositionResolver.FileMatches> matches() {
              return ImmutableList.of(matches);
            }

            @Override
            public int scannedDataFiles() {
              return 0;
            }
          };
        };

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null, resolver);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    assertThat(converter.scannedDataFiles()).isEqualTo(0);
    assertThat(result.dvFiles()).hasSize(1);
    assertThat(result.dvFiles().get(0).referencedDataFile()).isEqualTo(dataFile.location());
    assertThat(result.dvFiles().get(0).recordCount()).isEqualTo(1L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).containsExactly("1|a|v1");
  }

  @Test
  public void testSplitIntoRanges() {
    // 200 consecutive values and a cluster of 100 far away
    List<Object> values = Lists.newArrayList();
    for (long value = 0; value < 200; value++) {
      values.add(value);
    }
    for (long value = 1000; value < 1100; value++) {
      values.add(value);
    }

    List<int[]> ranges =
        KeyFilters.splitIntoRanges(values, value -> ((Number) value).doubleValue());
    assertThat(ranges).hasSize(KeyFilters.MAX_KEY_RANGES);
    assertThat(ranges.get(0)[0]).isEqualTo(0);
    assertThat(ranges.get(ranges.size() - 1)[1]).isEqualTo(values.size() - 1);
    for (int i = 1; i < ranges.size(); i++) {
      assertThat(ranges.get(i)[0]).isEqualTo(ranges.get(i - 1)[1] + 1);
    }
    // no range spans the gap between 199 and 1000
    assertThat(ranges)
        .noneMatch(
            range -> (long) values.get(range[0]) <= 199L && (long) values.get(range[1]) >= 1000L);

    // without a numeric converter the values are split into ranges of equal size
    List<int[]> equalRanges = KeyFilters.splitIntoRanges(values, null);
    assertThat(equalRanges).hasSize(KeyFilters.MAX_KEY_RANGES);
    assertThat(equalRanges).allMatch(range -> range[1] - range[0] + 1 == 3);

    // fewer values than ranges: one range per value
    List<int[]> single = KeyFilters.splitIntoRanges(values.subList(0, 5), null);
    assertThat(single).hasSize(5);
    assertThat(single).allMatch(range -> range[0] == range[1]);
  }

  @Test
  public void testConvertOnBranch() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult first = write(table, insert(1L, "a", "v1"), insert(2L, "a", "v1"));
    RowDelta rowDelta = table.newRowDelta().toBranch("staging");
    Arrays.stream(first.dataFiles()).forEach(rowDelta::addRows);
    rowDelta.commit();

    WriteResult second = write(table, delete(1L, "a"));

    // main has no snapshot, nothing to resolve against
    EqualityDeleteConverter.Result onMain =
        new EqualityDeleteConverter(table, null).convert(Arrays.asList(second.deleteFiles()));
    assertThat(onMain.baseSnapshotId()).isNull();
    assertThat(onMain.dvFiles()).isEmpty();

    EqualityDeleteConverter.Result onBranch =
        new EqualityDeleteConverter(table, "staging").convert(Arrays.asList(second.deleteFiles()));
    assertThat(onBranch.baseSnapshotId()).isEqualTo(table.snapshot("staging").snapshotId());
    assertThat(onBranch.dvFiles()).hasSize(1);
    assertThat(onBranch.dvFiles().get(0).recordCount()).isEqualTo(1L);
  }

  @Test
  public void testRejectsNonEqualityDeletes() {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult first = write(table, insert(1L, "a", "v1"));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());

    DeleteFile dv =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withFormat(FileFormat.PUFFIN)
            .withPath("/path/to/dv.puffin")
            .withFileSizeInBytes(10L)
            .withRecordCount(1L)
            .withReferencedDataFile(first.dataFiles()[0].location())
            .withContentOffset(4L)
            .withContentSizeInBytes(6L)
            .build();

    assertThatThrownBy(() -> new EqualityDeleteConverter(table, null).convert(ImmutableList.of(dv)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Not an equality delete file: /path/to/dv.puffin");
  }

  private Table createTable(PartitionSpec spec) {
    return catalog.createTable(
        TABLE_IDENTIFIER, SCHEMA, spec, ImmutableMap.of("format-version", "3"));
  }

  private static Object[] insert(Long id, String category, String data) {
    return new Object[] {Operation.INSERT, row(id, category, data)};
  }

  private static Object[] update(Long id, String category, String data) {
    return new Object[] {Operation.UPDATE, row(id, category, data)};
  }

  private static Object[] delete(Long id, String category) {
    return new Object[] {Operation.DELETE, row(id, category, null)};
  }

  private static Record row(Long id, String category, String data) {
    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", id);
    row.setField("category", category);
    row.setField("data", data);
    return row;
  }

  private WriteResult write(Table table, Object[]... operations) {
    BaseDeltaWriter writer =
        (BaseDeltaWriter) RecordUtils.createTableWriter(table, TABLE_REFERENCE, config);
    try {
      for (Object[] operation : operations) {
        writer.write((Record) operation[1], (Operation) operation[0]);
      }
      return writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void commit(
      Table table,
      DataFile[] dataFiles,
      Iterable<DeleteFile> addedDeletes,
      Iterable<DeleteFile> removedDeletes) {
    RowDelta rowDelta = table.newRowDelta();
    if (table.currentSnapshot() != null) {
      // replacing a deletion vector requires the starting snapshot, like the coordinator sets it
      rowDelta.validateFromSnapshot(table.currentSnapshot().snapshotId());
    }
    Arrays.stream(dataFiles).forEach(rowDelta::addRows);
    addedDeletes.forEach(rowDelta::addDeletes);
    removedDeletes.forEach(rowDelta::removeDeletes);
    rowDelta.commit();
  }

  private static void commit(
      Table table,
      DataFile[] dataFiles,
      DeleteFile[] addedDeletes,
      Iterable<DeleteFile> removedDeletes) {
    commit(table, dataFiles, Arrays.asList(addedDeletes), removedDeletes);
  }

  private static Set<String> readRows(Table table) {
    Set<String> rows = Sets.newHashSet();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(
            record.getField("id")
                + "|"
                + record.getField("category")
                + "|"
                + record.getField("data"));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }
}
