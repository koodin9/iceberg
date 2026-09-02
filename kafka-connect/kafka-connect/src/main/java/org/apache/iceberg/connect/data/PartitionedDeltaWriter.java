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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;

/**
 * Delta writer for partitioned tables. Records arrive in arbitrary partition order, so one delta
 * writer is kept open per partition. Deletes are written into the partition of the record, which is
 * why {@link RecordUtils} requires every partition source column to be an identifier field.
 */
class PartitionedDeltaWriter extends BaseDeltaWriter {

  private final PartitionKey partitionKey;
  private final Map<PartitionKey, RecordDeltaWriter> writers = Maps.newHashMap();

  PartitionedDeltaWriter(
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
    super(
        spec,
        format,
        writerFactory,
        fileFactory,
        io,
        targetFileSize,
        schema,
        equalityFieldIds,
        upsert,
        useDv);
    this.partitionKey = new PartitionKey(spec, schema);
  }

  @Override
  RecordDeltaWriter route(Record row) {
    partitionKey.partition(wrapper().wrap(row));

    RecordDeltaWriter writer = writers.get(partitionKey);
    if (writer == null) {
      // copy the key, the shared instance is mutated by the next record
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RecordDeltaWriter(copiedKey, dvFileWriter());
      writers.put(copiedKey, writer);
    }

    return writer;
  }

  @Override
  public void close() throws IOException {
    Tasks.foreach(writers.values())
        .throwFailureWhenFinished()
        .noRetry()
        .run(RecordDeltaWriter::close, IOException.class);
    writers.clear();
    super.close();
  }
}
