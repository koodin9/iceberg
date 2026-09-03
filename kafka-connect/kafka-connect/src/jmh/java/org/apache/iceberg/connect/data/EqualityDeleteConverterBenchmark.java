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
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Measures one equality delete conversion against a table with many data files, for key sets that
 * are clustered in the newest files, spread evenly over the table, or a mix of both. The counters
 * report how many data files the conversion read; {@link #scanAllKeyColumns} is the cost of reading
 * the key column of every file, i.e. a conversion without any pruning. JMH sums the counters over
 * the measurement iterations, so divide them by {@code Cnt} for the value of one conversion; the
 * log also reports every conversion.
 *
 * <p>The table is written once per (dataFiles, rowsPerFile) into the JVM's temporary directory and
 * reused by later runs. Delete it to start over.
 *
 * <p>Run with {@code ./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:jmh
 * -PjmhIncludeRegex=EqualityDeleteConverterBenchmark
 * -PjmhOutputPath=benchmark/eq-delete-converter.txt}
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class EqualityDeleteConverterBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(EqualityDeleteConverterBenchmark.class);

  @Param("2000")
  private int dataFiles;

  @Param("1000")
  private int rowsPerFile;

  @Param({"200", "2000", "20000"})
  private int deletedKeys;

  // HOT: the newest keys, SCATTERED: evenly spread over the table, MIXED: 90% hot and 10% scattered
  @Param({"HOT", "SCATTERED", "MIXED"})
  private String distribution;

  private File tableDir;
  private Table table;
  private List<DeleteFile> eqDeleteFiles;

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    File baseDir = new File(System.getProperty("java.io.tmpdir"), "kc-eq-delete-converter-bench");
    this.tableDir = new File(baseDir, String.format(Locale.ROOT, "%dx%d", dataFiles, rowsPerFile));
    this.table = BenchmarkTables.createOrLoad(tableDir);

    if (table.currentSnapshot() == null) {
      LOG.info("Writing {} data files with {} rows each to {}", dataFiles, rowsPerFile, tableDir);
      List<DataFile> files = Lists.newArrayList();
      for (int file = 0; file < dataFiles; file++) {
        files.add(BenchmarkTables.writeDataFile(table, (long) file * rowsPerFile, rowsPerFile));
      }

      BenchmarkTables.commit(table, files, ImmutableList.of(), ImmutableList.of(), null);
    }

    this.eqDeleteFiles = BenchmarkTables.writeEqualityDeletes(table, keys());
    LOG.info("Converting {} {} keys against {} data files", deletedKeys, distribution, dataFiles);
  }

  @TearDown(Level.Trial)
  public void tearDownBenchmark() {
    for (DeleteFile deleteFile : eqDeleteFiles) {
      table.io().deleteFile(deleteFile.location());
    }
  }

  @Benchmark
  @Threads(1)
  public void convert(Blackhole blackhole, Counters counters) {
    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(eqDeleteFiles);

    counters.add(converter.scannedDataFiles(), result.dvFiles().size());
    LOG.info(
        "{} {} keys: scanned {} of {} data files, wrote {} deletion vectors",
        deletedKeys,
        distribution,
        converter.scannedDataFiles(),
        dataFiles,
        result.dvFiles().size());
    // the vectors are not committed; drop the Puffin file so runs do not accumulate files
    if (!result.dvFiles().isEmpty()) {
      table.io().deleteFile(result.dvFiles().get(0).location());
    }

    blackhole.consume(result);
  }

  /** Reads the key column of every data file: the cost of a conversion without pruning. */
  @Benchmark
  @Threads(1)
  public void scanAllKeyColumns(Blackhole blackhole) throws IOException {
    long count = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).select("id").build()) {
      for (Record record : records) {
        count += (long) record.getField("id");
      }
    }

    blackhole.consume(count);
  }

  private List<Long> keys() {
    long total = (long) dataFiles * rowsPerFile;
    List<Long> keys = Lists.newArrayListWithCapacity(deletedKeys);
    switch (distribution) {
      case "HOT":
        for (long id = total - deletedKeys; id < total; id++) {
          keys.add(id);
        }
        break;

      case "SCATTERED":
        for (int i = 0; i < deletedKeys; i++) {
          keys.add(i * total / deletedKeys);
        }
        break;

      case "MIXED":
        int scattered = deletedKeys / 10;
        for (long id = total - (deletedKeys - scattered); id < total; id++) {
          keys.add(id);
        }
        for (int i = 0; i < scattered; i++) {
          keys.add(i * total / scattered);
        }
        break;

      default:
        throw new IllegalArgumentException("Unknown distribution: " + distribution);
    }

    return keys;
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class Counters {
    private long scannedDataFiles;
    private long deletionVectors;

    @Setup(Level.Iteration)
    public void reset() {
      this.scannedDataFiles = 0;
      this.deletionVectors = 0;
    }

    void add(long scanned, long vectors) {
      this.scannedDataFiles += scanned;
      this.deletionVectors += vectors;
    }

    public long scannedDataFiles() {
      return scannedDataFiles;
    }

    public long deletionVectors() {
      return deletionVectors;
    }
  }
}
