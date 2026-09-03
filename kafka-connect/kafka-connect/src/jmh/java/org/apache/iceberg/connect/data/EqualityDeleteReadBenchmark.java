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
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures a full read of a table that received the same CDC commits twice: once with the equality
 * delete files committed as the sink writes them, once with the deletes converted into deletion
 * vectors at commit time. Every commit inserts new rows and updates rows of earlier commits, so the
 * equality delete variant accumulates one equality delete file per commit that every later read has
 * to apply to the older data files.
 *
 * <p>The tables are written once per (commits, deleteMode) into the JVM's temporary directory and
 * reused by later runs. Delete them to start over. JMH sums the counters over the measurement
 * iterations, so divide them by {@code Cnt} for the value of one read.
 *
 * <p>Run with {@code ./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:jmh
 * -PjmhIncludeRegex=EqualityDeleteReadBenchmark -PjmhOutputPath=benchmark/eq-delete-read.txt}
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class EqualityDeleteReadBenchmark {

  @Param({"100", "400"})
  private int commits;

  @Param({BenchmarkTables.EQUALITY_DELETES, BenchmarkTables.DELETION_VECTORS})
  private String deleteMode;

  private Table table;

  @Setup(Level.Trial)
  public void setupBenchmark() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"), "kc-eq-delete-read-bench");
    File tableDir = new File(baseDir, String.format(Locale.ROOT, "%d-%s", commits, deleteMode));
    this.table = BenchmarkTables.cdcTable(tableDir, commits, deleteMode);
  }

  @Benchmark
  @Threads(1)
  public void readTable(Blackhole blackhole, Counters counters) throws IOException {
    long rows = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows++;
        blackhole.consume(record.getField("payload"));
      }
    }

    counters.add(
        rows,
        Long.parseLong(
            table
                .currentSnapshot()
                .summary()
                .getOrDefault(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")));
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class Counters {
    private long rows;
    private long deleteFiles;

    @Setup(Level.Iteration)
    public void reset() {
      this.rows = 0;
      this.deleteFiles = 0;
    }

    void add(long readRows, long tableDeleteFiles) {
      this.rows += readRows;
      this.deleteFiles += tableDeleteFiles;
    }

    public long rows() {
      return rows;
    }

    public long deleteFiles() {
      return deleteFiles;
    }
  }
}
