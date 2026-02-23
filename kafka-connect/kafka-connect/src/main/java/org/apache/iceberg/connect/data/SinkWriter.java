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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.cmdb.CmdbManager;
import org.apache.iceberg.connect.cmdb.CmdbManagerFactory;
import org.apache.iceberg.connect.cmdb.dto.dml.request.DmlStatusPatchRequest;
import org.apache.iceberg.connect.cmdb.model.DmlInfo;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkWriter {
  private static final Logger LOG = LoggerFactory.getLogger(SinkWriter.class);

  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> sourceOffsets;
  private final Map<Integer, SinkRecord> lastProcessedRecords;
  private final CmdbManager cmdbManager;

  public SinkWriter(Catalog catalog, IcebergSinkConfig config) {
    this.config = config;
    this.writerFactory = new IcebergWriterFactory(catalog, config);
    this.writers = Maps.newHashMap();
    this.sourceOffsets = Maps.newHashMap();
    this.lastProcessedRecords = Maps.newHashMap();
    this.cmdbManager = CmdbManagerFactory.getInstance(config);
  }

  public void close() {
    writers.values().forEach(RecordWriter::close);
  }

  public void putSourceOffset(TopicPartition topicPartition, Offset offset) {
    sourceOffsets.put(topicPartition, offset);
  }

  public SinkWriterResult completeWrite() {
    LOG.info("[DDL connector] SinkWriter completeWrite called, writers count: {}", writers.size());

    List<IcebergWriterResult> writerResults =
        writers.values().stream()
            .flatMap(writer -> writer.complete().stream())
            .collect(Collectors.toList());
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    long totalRecords =
        writerResults.stream()
            .mapToLong(wr -> wr.dataFiles().stream().mapToLong(ContentFile::recordCount).sum())
            .sum();
    LOG.info(
        "[DDL connector] Worker committable writeResults: {} results, {} total records",
        writerResults.size(),
        totalRecords);

    writers.clear();
    sourceOffsets.clear();

    if (!lastProcessedRecords.isEmpty()) {
      LOG.info(
          "[DDL connector] Save last processed DML info. lastProcessedRecords : {}",
          lastProcessedRecords);
      saveLastProcessedDMLInfo(lastProcessedRecords);
      lastProcessedRecords.clear();
    }

    return new SinkWriterResult(writerResults, offsets);
  }

  public void saveLastProcessedDMLInfo(Map<Integer, SinkRecord> records) {
    for (SinkRecord record : records.values()) {
      LOG.info("[DDL connector] saveLastProcessedDMLInfo record: {}", record);

      // primary key (id) 획득
      long partition = record.kafkaPartition().longValue();
      long ddlVersion = ddlVersionFromHeader(record);
      Long recordId = cmdbManager.findDmlConsumerStatusId(partition, ddlVersion);

      Object dmlObj = record.headers().lastWithName("dml_info").value();
      if (dmlObj instanceof Map<?, ?> mapValue) {
        // consumer status 업데이트
        DmlInfo dmlInfo =
            new DmlInfo(
                Map.of(
                    RecordUtils.POS_KEY, mapValue.get(RecordUtils.POS_KEY).toString(),
                    RecordUtils.ROW_KEY, mapValue.get(RecordUtils.ROW_KEY).toString(),
                    RecordUtils.GTID_KEY, mapValue.get(RecordUtils.GTID_KEY).toString()));

        if (recordId == null) {
          // partition, ddlVersion에 해당하는 consumer status가 없으면 신규생성
          cmdbManager.postDmlConsumerStatus(partition, record.kafkaOffset(), ddlVersion, dmlInfo);
        } else {
          // partition, ddlVersion에 해당하는 consumer status가 있으면 업데이트
          cmdbManager.patchDmlConsumerStatus(
              recordId, new DmlStatusPatchRequest(record.kafkaOffset(), dmlInfo.toJson(), null));
        }

        LOG.info(
            "[DDL connector] Last DML record saved. Partition: {}, Offset: {}",
            partition,
            record.kafkaOffset());
      }
    }
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    sinkRecords.forEach(this::save);
  }

  public void save(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    OffsetDateTime timestamp =
        record.timestamp() == null
            ? null
            : OffsetDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneOffset.UTC);
    sourceOffsets.put(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, timestamp));

    if (config.dynamicTablesEnabled()) {
      routeRecordDynamically(record);
    } else {
      routeRecordStatically(record);
    }
  }

  private void routeRecordStatically(SinkRecord record) {
    String routeField = config.tablesRouteField();

    if (routeField == null) {
      // route to all tables
      config
          .tables()
          .forEach(
              tableName -> {
                writerForTable(tableName, record, false).write(record);
              });

    } else {
      String routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        config
            .tables()
            .forEach(
                tableName -> {
                  Pattern regex = config.tableConfig(tableName).routeRegex();
                  if (regex != null && regex.matcher(routeValue).matches()) {
                    writerForTable(tableName, record, false).write(record);
                  }
                });
      }
    }
  }

  private void routeRecordDynamically(SinkRecord record) {
    String routeField = config.tablesRouteField();
    Preconditions.checkNotNull(routeField, "Route field cannot be null with dynamic routing");

    String routeValue = extractRouteValue(record.value(), routeField);
    if (routeValue != null) {
      String tableName = routeValue.toLowerCase(Locale.ROOT);
      writerForTable(tableName, record, true).write(record);
    } else {
      LOG.warn("[Dynamic routing] routeValue is null, record will be skipped");
    }
  }

  private String extractRouteValue(Object recordValue, String routeField) {
    if (recordValue == null) {
      return null;
    }
    Object routeValue = RecordUtils.extractFromRecordValue(recordValue, routeField);
    return routeValue == null ? null : routeValue.toString();
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }

  public Long ddlVersionFromHeader(SinkRecord record) {
    Header ddlVersionHeader = record.headers().lastWithName("ddl_version");
    if (ddlVersionHeader == null || ddlVersionHeader.value() == null) {
      LOG.error("[DDL connector] Missing or null 'ddl_version' header in the record.");
      throw new RuntimeException("Missing or null 'ddl_version' header in the record.");
    }
    return Long.parseLong(RecordUtils.headerValueToString(ddlVersionHeader.value()));
  }
}
