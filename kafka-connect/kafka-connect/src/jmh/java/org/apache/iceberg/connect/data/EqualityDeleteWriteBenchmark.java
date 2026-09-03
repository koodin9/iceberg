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
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures one more CDC commit on a table that already holds many commits: writing a batch of
 * inserts and updates, then committing it with the equality delete files as written or with the
 * deletes converted into deletion vectors. The updated keys either belong to the newest commits, as
 * in a stream that mostly changes recent rows, or are spread over the whole table. After every
 * measurement the table is rolled back to its base snapshot and the written files are removed, so
 * every iteration sees the same table.
 *
 * <p>The base tables are written once per (commits, deleteMode) into the JVM's temporary directory
 * and reused by later runs. JMH sums the counters over the measurement iterations, so divide them
 * by {@code Cnt} for the value of one commit.
 *
 * <p>Run with {@code ./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:jmh
 * -PjmhIncludeRegex=EqualityDeleteWriteBenchmark -PjmhOutputPath=benchmark/eq-delete-write.txt}
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class EqualityDeleteWriteBenchmark {

  // updates of the last commits' rows are written to the newest data files
  private static final int RECENT_COMMITS = 3;

  @Param({"100", "400"})
  private int commits;

  @Param({BenchmarkTables.EQUALITY_DELETES, BenchmarkTables.DELETION_VECTORS})
  private String deleteMode;

  // RECENT: keys of the last commits, RANDOM: keys spread over the whole table
  @Param({"RECENT", "RANDOM"})
  private String updates;

  private Table table;
  private long baseSnapshotId;
  private long nextId;
  private Random random;
  private final List<String> writtenFiles = Lists.newArrayList();

  @Setup(Level.Trial)
  public void setupBenchmark() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"), "kc-eq-delete-write-bench");
    File tableDir = new File(baseDir, String.format(Locale.ROOT, "%d-%s", commits, deleteMode));
    this.table = BenchmarkTables.cdcTable(tableDir, commits, deleteMode);
    this.baseSnapshotId = table.currentSnapshot().snapshotId();
    this.nextId = (long) commits * BenchmarkTables.ROWS_PER_COMMIT;
  }

  @Setup(Level.Iteration)
  public void setupIteration() {
    this.random = new Random(7L);
  }

  @Benchmark
  @Threads(1)
  public void commit(Blackhole blackhole, Counters counters) {
    long recentFrom = nextId - (long) RECENT_COMMITS * BenchmarkTables.ROWS_PER_COMMIT;
    WriteResult result =
        BenchmarkTables.writeBatch(
            table,
            nextId,
            BenchmarkTables.ROWS_PER_COMMIT,
            () ->
                updates.equals("RECENT")
                    ? recentFrom + (long) (random.nextDouble() * (nextId - recentFrom))
                    : (long) (random.nextDouble() * nextId),
            "updated by benchmark");
    for (ContentFile<?> file : result.dataFiles()) {
      writtenFiles.add(file.location());
    }
    for (ContentFile<?> file : result.deleteFiles()) {
      writtenFiles.add(file.location());
    }

    EqualityDeleteConverter.Result converted =
        BenchmarkTables.commitBatch(table, result, deleteMode);
    if (converted != null) {
      counters.add(converted.dvFiles().size());
      if (!converted.dvFiles().isEmpty()) {
        writtenFiles.add(converted.dvFiles().get(0).location());
      }
    }

    blackhole.consume(result);
  }

  /** Puts the table back to its base snapshot and drops the files the iteration wrote. */
  @TearDown(Level.Iteration)
  public void rollback() {
    table.manageSnapshots().rollbackTo(baseSnapshotId).commit();
    for (String location : writtenFiles) {
      table.io().deleteFile(location);
    }

    writtenFiles.clear();
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class Counters {
    private long deletionVectors;

    @Setup(Level.Iteration)
    public void reset() {
      this.deletionVectors = 0;
    }

    void add(long vectors) {
      this.deletionVectors += vectors;
    }

    public long deletionVectors() {
      return deletionVectors;
    }
  }
}
