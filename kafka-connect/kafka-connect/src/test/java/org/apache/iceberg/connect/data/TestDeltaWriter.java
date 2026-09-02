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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestDeltaWriter {

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
          ImmutableSet.of(1, 2));

  private static final PartitionSpec CATEGORY_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("category").build();

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
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @ParameterizedTest
  @CsvSource({"2, false", "2, true", "3, false", "3, true"})
  public void testUpsert(int formatVersion, boolean partitioned) throws IOException {
    when(config.upsertModeEnabled()).thenReturn(true);
    Table table = createTable(formatVersion, partitioned ? CATEGORY_SPEC : null);

    BaseDeltaWriter writer = createWriter(table, partitioned);
    writer.write(row(1L, "a", "v1"));
    writer.write(row(2L, "b", "v1"));
    WriteResult first = writer.complete();

    // upsert mode writes a key delete for every insert, even for a key that is new to the table
    assertThat(first.dataFiles()).hasSize(partitioned ? 2 : 1);
    assertThat(first.deleteFiles())
        .allMatch(file -> file.content() == FileContent.EQUALITY_DELETES);
    commit(table, first);

    writer = createWriter(table, partitioned);
    // upsert of a row from the previous commit, removed with an equality delete
    writer.write(row(1L, "a", "v2"));
    // duplicate key within the batch, removed with a position delete
    writer.write(row(3L, "c", "v1"), Operation.INSERT);
    writer.write(row(3L, "c", "v2"), Operation.INSERT);
    // delete of a row from the previous commit
    writer.write(row(2L, "b", null), Operation.DELETE);
    WriteResult second = writer.complete();

    List<DeleteFile> positionDeletes =
        deleteFiles(second, file -> file.content() == FileContent.POSITION_DELETES);
    assertThat(positionDeletes).hasSize(1);
    assertThat(deleteFiles(second, file -> file.content() == FileContent.EQUALITY_DELETES))
        .isNotEmpty();
    if (formatVersion >= 3) {
      assertThat(positionDeletes).allMatch(ContentFileUtil::isDV);
      assertThat(positionDeletes).allMatch(file -> file.format() == FileFormat.PUFFIN);
    } else {
      assertThat(positionDeletes).noneMatch(ContentFileUtil::isDV);
    }
    commit(table, second);

    assertThat(readRows(table)).containsExactlyInAnyOrder("1|a|v2", "3|c|v2");
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void testCdcOperations(int formatVersion) throws IOException {
    when(config.tablesCdcField()).thenReturn("_cdc.op");
    Table table = createTable(formatVersion, CATEGORY_SPEC);

    BaseDeltaWriter writer = createWriter(table, true);
    writer.write(row(1L, "a", "v1"), Operation.INSERT);
    writer.write(row(2L, "a", "v1"), Operation.INSERT);
    writer.write(row(3L, "b", "v1"), Operation.INSERT);
    WriteResult first = writer.complete();

    // without upsert mode an insert writes no delete
    assertThat(first.deleteFiles()).isEmpty();
    commit(table, first);

    writer = createWriter(table, true);
    writer.write(row(1L, "a", "v2"), Operation.UPDATE);
    writer.write(row(2L, "a", null), Operation.DELETE);
    writer.write(row(4L, "b", "v1"), Operation.INSERT);
    WriteResult second = writer.complete();

    // an update and a delete in partition a share one equality delete file
    assertThat(second.deleteFiles()).hasSize(1);
    assertThat(second.deleteFiles()[0].content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(second.deleteFiles()[0].equalityFieldIds()).containsExactlyInAnyOrder(1, 2);
    commit(table, second);

    assertThat(readRows(table)).containsExactlyInAnyOrder("1|a|v2", "3|b|v1", "4|b|v1");
  }

  @Test
  public void testIdColumnsOverrideIdentifierFields() throws IOException {
    when(config.tablesCdcField()).thenReturn("_cdc.op");
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.idColumns()).thenReturn(ImmutableList.of("id"));
    when(config.tableConfig(any())).thenReturn(tableConfig);
    Table table = createTable(3, null);

    BaseDeltaWriter writer = createWriter(table, false);
    writer.write(row(1L, "a", "v1"), Operation.INSERT);
    commit(table, writer.complete());

    writer = createWriter(table, false);
    // the category differs, the row is still matched by id only
    writer.write(row(1L, "b", null), Operation.DELETE);
    WriteResult result = writer.complete();

    assertThat(result.deleteFiles()).hasSize(1);
    assertThat(result.deleteFiles()[0].equalityFieldIds()).containsExactly(1);
    commit(table, result);

    assertThat(readRows(table)).isEmpty();
  }

  @Test
  public void testPartitionColumnsMustBeIdentifierFields() {
    when(config.upsertModeEnabled()).thenReturn(true);
    Table table = createTable(3, PartitionSpec.builderFor(SCHEMA).identity("data").build());

    assertThatThrownBy(() -> RecordUtils.createTableWriter(table, TABLE_REFERENCE, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Partition field data uses source column data");
  }

  @Test
  public void testDeltaModeRequiresIdentifierFields() {
    when(config.upsertModeEnabled()).thenReturn(true);
    Schema schema = new Schema(SCHEMA.columns());
    Table table =
        catalog.createTable(
            TABLE_IDENTIFIER,
            schema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", "3"));

    assertThatThrownBy(() -> RecordUtils.createTableWriter(table, TABLE_REFERENCE, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no identifier fields");
  }

  @Test
  public void testAppendWriterWithoutDeltaMode() throws IOException {
    Table table = createTable(3, null);

    // identifier fields alone do not switch to the delta writer
    try (TaskWriter<Record> writer =
        RecordUtils.createTableWriter(table, TABLE_REFERENCE, config)) {
      assertThat(writer).isInstanceOf(UnpartitionedWriter.class);
    }
  }

  @Test
  public void testOperationFromString() {
    assertThat(Operation.fromString("I")).isEqualTo(Operation.INSERT);
    assertThat(Operation.fromString("c")).isEqualTo(Operation.INSERT);
    assertThat(Operation.fromString("r")).isEqualTo(Operation.INSERT);
    assertThat(Operation.fromString("insert")).isEqualTo(Operation.INSERT);
    assertThat(Operation.fromString("U")).isEqualTo(Operation.UPDATE);
    assertThat(Operation.fromString("update")).isEqualTo(Operation.UPDATE);
    assertThat(Operation.fromString("D")).isEqualTo(Operation.DELETE);
    assertThat(Operation.fromString(" delete ")).isEqualTo(Operation.DELETE);

    assertThatThrownBy(() -> Operation.fromString("x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid CDC operation: x");
    assertThatThrownBy(() -> Operation.fromString(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid CDC operation: null");
  }

  private Table createTable(int formatVersion, PartitionSpec spec) {
    return catalog.createTable(
        TABLE_IDENTIFIER,
        SCHEMA,
        spec == null ? PartitionSpec.unpartitioned() : spec,
        ImmutableMap.of("format-version", String.valueOf(formatVersion)));
  }

  private BaseDeltaWriter createWriter(Table table, boolean partitioned) {
    TaskWriter<Record> writer = RecordUtils.createTableWriter(table, TABLE_REFERENCE, config);
    assertThat(writer)
        .isInstanceOf(partitioned ? PartitionedDeltaWriter.class : UnpartitionedDeltaWriter.class);
    return (BaseDeltaWriter) writer;
  }

  private static Record row(Long id, String category, String data) {
    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", id);
    row.setField("category", category);
    row.setField("data", data);
    return row;
  }

  private static List<DeleteFile> deleteFiles(WriteResult result, Predicate<DeleteFile> predicate) {
    return Arrays.stream(result.deleteFiles()).filter(predicate).collect(Collectors.toList());
  }

  private static void commit(Table table, WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    for (DataFile dataFile : result.dataFiles()) {
      rowDelta.addRows(dataFile);
    }
    for (DeleteFile deleteFile : result.deleteFiles()) {
      rowDelta.addDeletes(deleteFile);
    }
    rowDelta.commit();
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
