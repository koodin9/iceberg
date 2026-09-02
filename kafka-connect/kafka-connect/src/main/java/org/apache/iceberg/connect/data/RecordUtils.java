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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

class RecordUtils {

  @SuppressWarnings("unchecked")
  static Object extractFromRecordValue(Object recordValue, String fieldName) {
    List<String> fields = Splitter.on('.').splitToList(fieldName);
    if (recordValue instanceof Struct) {
      return valueFromStruct((Struct) recordValue, fields);
    } else if (recordValue instanceof Map) {
      return valueFromMap((Map<String, ?>) recordValue, fields);
    } else {
      throw new UnsupportedOperationException(
          "Cannot extract value from type: " + recordValue.getClass().getName());
    }
  }

  private static Object valueFromStruct(Struct parent, List<String> fields) {
    Struct struct = parent;
    for (int idx = 0; idx < fields.size() - 1; idx++) {
      Object value = fieldValueFromStruct(struct, fields.get(idx));
      if (value == null) {
        return null;
      }
      Preconditions.checkState(value instanceof Struct, "Expected a struct type");
      struct = (Struct) value;
    }
    return fieldValueFromStruct(struct, fields.get(fields.size() - 1));
  }

  private static Object fieldValueFromStruct(Struct struct, String fieldName) {
    Field structField = struct.schema().field(fieldName);
    if (structField == null) {
      return null;
    }
    return struct.get(structField);
  }

  @SuppressWarnings("unchecked")
  private static Object valueFromMap(Map<String, ?> parent, List<String> fields) {
    Map<String, ?> map = parent;
    for (int idx = 0; idx < fields.size() - 1; idx++) {
      Object value = map.get(fields.get(idx));
      if (value == null) {
        return null;
      }
      Preconditions.checkState(value instanceof Map, "Expected a map type");
      map = (Map<String, ?>) value;
    }
    return map.get(fields.get(fields.size() - 1));
  }

  public static TaskWriter<Record> createTableWriter(
      Table table, TableReference tableReference, IcebergSinkConfig config) {
    Map<String, String> tableProps = Maps.newHashMap(table.properties());
    tableProps.putAll(config.writeProps());

    String formatStr =
        tableProps.getOrDefault(
            TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.fromString(formatStr);

    long targetFileSize =
        PropertyUtil.propertyAsLong(
            tableProps,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();

    // override the identifier fields if the config is set
    List<String> idCols = config.tableConfig(tableReference.identifier().toString()).idColumns();
    if (!idCols.isEmpty()) {
      identifierFieldIds =
          idCols.stream()
              .map(
                  colName -> {
                    NestedField field = table.schema().findField(colName);
                    if (field == null) {
                      throw new IllegalArgumentException("ID column not found: " + colName);
                    }
                    return field.fieldId();
                  })
              .collect(Collectors.toSet());
    }

    boolean deltaMode = config.tablesCdcField() != null || config.upsertModeEnabled();
    Preconditions.checkArgument(
        !deltaMode || (identifierFieldIds != null && !identifierFieldIds.isEmpty()),
        "Table %s has no identifier fields, set id-columns to use CDC or upsert mode",
        tableReference.identifier());

    FileWriterFactory<Record> writerFactory;
    if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      writerFactory =
          new GenericFileWriterFactory.Builder(table)
              .dataSchema(table.schema())
              .dataFileFormat(format)
              .writerProperties(tableProps)
              .build();
    } else {
      writerFactory =
          new GenericFileWriterFactory.Builder(table)
              .dataSchema(table.schema())
              .dataFileFormat(format)
              .equalityFieldIds(Ints.toArray(identifierFieldIds))
              .equalityDeleteRowSchema(
                  TypeUtil.select(table.schema(), Sets.newHashSet(identifierFieldIds)))
              .deleteFileFormat(format)
              .writerProperties(tableProps)
              .build();
    }

    // (partition ID + task ID + operation ID) must be unique
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    if (deltaMode) {
      return createDeltaWriter(
          table, config, format, writerFactory, fileFactory, targetFileSize, identifierFieldIds);
    }

    TaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      writer =
          new UnpartitionedWriter<>(
              table.spec(), format, writerFactory, fileFactory, table.io(), targetFileSize);
    } else {
      writer =
          new PartitionedAppendWriter(
              table.spec(),
              format,
              writerFactory,
              fileFactory,
              table.io(),
              targetFileSize,
              table.schema());
    }
    return writer;
  }

  private static TaskWriter<Record> createDeltaWriter(
      Table table,
      IcebergSinkConfig config,
      FileFormat format,
      FileWriterFactory<Record> writerFactory,
      OutputFileFactory fileFactory,
      long targetFileSize,
      Set<Integer> equalityFieldIds) {
    // format version 3 requires deletion vectors for position deletes
    boolean useDv = TableUtil.formatVersion(table) >= 3;
    boolean upsert = config.upsertModeEnabled();

    if (table.spec().isUnpartitioned()) {
      return new UnpartitionedDeltaWriter(
          table.spec(),
          format,
          writerFactory,
          fileFactory,
          table.io(),
          targetFileSize,
          table.schema(),
          equalityFieldIds,
          upsert,
          useDv);
    }

    // a delete is written into the partition of the record, so the partition must be derivable
    // from the identifier fields or the delete would miss rows in other partitions
    for (PartitionField field : table.spec().fields()) {
      Preconditions.checkArgument(
          equalityFieldIds.contains(field.sourceId()),
          "Partition field %s uses source column %s which is not an identifier field, "
              + "partition columns must be a subset of the id-columns for CDC or upsert mode",
          field.name(),
          table.schema().findColumnName(field.sourceId()));
    }

    return new PartitionedDeltaWriter(
        table.spec(),
        format,
        writerFactory,
        fileFactory,
        table.io(),
        targetFileSize,
        table.schema(),
        equalityFieldIds,
        upsert,
        useDv);
  }

  private RecordUtils() {}
}
