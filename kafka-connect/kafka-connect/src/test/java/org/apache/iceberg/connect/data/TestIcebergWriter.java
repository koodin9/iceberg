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
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergWriter {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");

  private static final org.apache.iceberg.Schema SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));

  private static final Schema CDC_SCHEMA =
      SchemaBuilder.struct().field("op", Schema.STRING_SCHEMA).build();
  private static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .field("id", Schema.INT64_SCHEMA)
          .field("data", Schema.OPTIONAL_STRING_SCHEMA)
          .field("_cdc", CDC_SCHEMA)
          .build();

  private InMemoryCatalog catalog;
  private Table table;
  private IcebergSinkConfig config;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test_catalog", ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table =
        catalog.createTable(
            TABLE_IDENTIFIER,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", "3"));

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
  public void testCdcOperationFromRecordValue() {
    IcebergWriter writer = new IcebergWriter(table, tableReference(), config);
    writer.write(record(1L, "v1", "I"));
    writer.write(record(2L, "v1", "I"));
    List<IcebergWriterResult> first = writer.complete();
    writer.close();

    assertThat(first).hasSize(1);
    assertThat(first.get(0).dataFiles()).hasSize(1);
    assertThat(first.get(0).deleteFiles()).isEmpty();

    // a new writer is created for every commit, see SinkWriter.completeWrite
    writer = new IcebergWriter(table, tableReference(), config);
    // update and delete of rows from the previous batch become equality deletes
    writer.write(record(1L, "v2", "U"));
    writer.write(record(2L, null, "D"));
    // a duplicate key within the batch becomes a position delete
    writer.write(record(3L, "v1", "I"));
    writer.write(record(3L, "v2", "I"));
    // a tombstone is ignored
    writer.write(new SinkRecord("topic", 0, null, null, null, null, 4));
    List<IcebergWriterResult> second = writer.complete();
    writer.close();

    assertThat(second).hasSize(1);
    assertThat(second.get(0).dataFiles()).hasSize(1);
    assertThat(second.get(0).deleteFiles())
        .anyMatch(file -> file.content() == FileContent.EQUALITY_DELETES)
        .anyMatch(file -> file.content() == FileContent.POSITION_DELETES);
  }

  @Test
  public void testInvalidCdcOperation() {
    IcebergWriter writer = new IcebergWriter(table, tableReference(), config);
    try {
      assertThatThrownBy(() -> writer.write(record(1L, "v1", "x")))
          .isInstanceOf(DataException.class)
          .hasMessageStartingWith("An error occurred converting record")
          .hasRootCauseMessage("Invalid CDC operation: x");
    } finally {
      writer.close();
    }
  }

  @Test
  public void testMissingCdcOperation() {
    when(config.tablesCdcField()).thenReturn("_cdc.missing");
    IcebergWriter writer = new IcebergWriter(table, tableReference(), config);
    try {
      assertThatThrownBy(() -> writer.write(record(1L, "v1", "I")))
          .isInstanceOf(DataException.class)
          .hasMessageStartingWith("An error occurred converting record")
          .hasRootCauseMessage("CDC field _cdc.missing is missing or null in the record");
    } finally {
      writer.close();
    }
  }

  private TableReference tableReference() {
    return TableReference.of("test_catalog", TABLE_IDENTIFIER, UUID.randomUUID());
  }

  private static SinkRecord record(long id, String data, String op) {
    Struct cdc = new Struct(CDC_SCHEMA).put("op", op);
    Struct value = new Struct(VALUE_SCHEMA).put("id", id).put("data", data).put("_cdc", cdc);
    return new SinkRecord("topic", 0, null, null, VALUE_SCHEMA, value, id);
  }
}
