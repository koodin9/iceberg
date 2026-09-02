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
import java.util.List;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Task writer that applies CDC operations to a table with identifier fields.
 *
 * <p>Inserts are written to data files. A delete, and the delete half of an update, is written as
 * an equality delete on the identifier fields so that a row committed earlier can be removed
 * without knowing its position. An equality delete only applies to data files with a lower sequence
 * number, so a row written earlier in the same batch is removed with a position delete instead: a
 * deletion vector on format version 3 tables, a position delete file on version 2 tables.
 *
 * <p>In upsert mode every insert is preceded by a delete of its key, which makes the sink
 * idempotent for re-delivered records at the cost of one equality delete per row.
 *
 * <p>Position deletes only cover rows written by this writer instance. A schema change in the
 * middle of a batch replaces the writer, so a duplicate key that straddles the change is not
 * deduplicated within that batch.
 */
abstract class BaseDeltaWriter extends BaseTaskWriter<Record> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final InternalRecordWrapper wrapper;
  private final InternalRecordWrapper keyWrapper;
  private final Record keyRecord;
  private final int[] keyPositions;
  private final boolean upsert;

  BaseDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<Record> writerFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> equalityFieldIds,
      boolean upsert,
      boolean useDv) {
    super(spec, format, writerFactory, fileFactory, io, targetFileSize, useDv);
    Preconditions.checkArgument(
        equalityFieldIds != null && !equalityFieldIds.isEmpty(),
        "Equality field IDs cannot be empty");
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, equalityFieldIds);
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
    this.keyRecord = GenericRecord.create(deleteSchema);
    this.keyPositions = keyPositions(schema, deleteSchema);
    this.upsert = upsert;
  }

  /**
   * Returns, for each column of the equality delete schema, the position of that column in the
   * table schema. Only top-level columns are supported because the key record is filled by
   * position.
   */
  private static int[] keyPositions(Schema schema, Schema deleteSchema) {
    List<Types.NestedField> columns = schema.columns();
    List<Types.NestedField> keyColumns = deleteSchema.columns();
    int[] positions = new int[keyColumns.size()];
    for (int keyPos = 0; keyPos < keyColumns.size(); keyPos++) {
      Types.NestedField keyColumn = keyColumns.get(keyPos);
      Types.NestedField column = schema.asStruct().field(keyColumn.fieldId());
      Preconditions.checkArgument(
          column != null && column.type().equals(keyColumn.type()),
          "Identifier field %s must be a top-level column",
          keyColumn.name());
      positions[keyPos] = columns.indexOf(column);
    }

    return positions;
  }

  abstract RecordDeltaWriter route(Record row);

  InternalRecordWrapper wrapper() {
    return wrapper;
  }

  /** Writes the row as an insert. Used when the record carries no CDC operation. */
  @Override
  public void write(Record row) throws IOException {
    write(row, Operation.INSERT);
  }

  public void write(Record row, Operation op) throws IOException {
    RecordDeltaWriter writer = route(row);

    switch (op) {
      case INSERT:
        if (upsert) {
          writer.deleteKey(keyOf(row));
        }
        writer.write(row);
        break;

      case UPDATE:
        writer.deleteKey(keyOf(row));
        writer.write(row);
        break;

      case DELETE:
        writer.deleteKey(keyOf(row));
        break;

      default:
        throw new UnsupportedOperationException("Unknown operation: " + op);
    }
  }

  /**
   * Projects the identifier fields of the row into the reusable key record. The equality delete
   * writer consumes the record before the next call, so a single instance is safe to reuse.
   */
  private Record keyOf(Record row) {
    for (int keyPos = 0; keyPos < keyPositions.length; keyPos++) {
      keyRecord.set(keyPos, row.get(keyPositions[keyPos], Object.class));
    }

    return keyRecord;
  }

  protected class RecordDeltaWriter extends BaseEqualityDeltaWriter {
    RecordDeltaWriter(StructLike partition, PartitioningDVWriter<Record> dvWriter) {
      super(partition, schema, deleteSchema, DeleteGranularity.FILE, dvWriter);
    }

    @Override
    protected StructLike asStructLike(Record data) {
      return wrapper.wrap(data);
    }

    @Override
    protected StructLike asStructLikeKey(Record key) {
      return keyWrapper.wrap(key);
    }
  }
}
