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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.cmdb.CmdbManager;
import org.apache.iceberg.connect.cmdb.dto.dml.response.DmlStatusResponse;
import org.apache.iceberg.connect.cmdb.model.DmlInfo;
import org.apache.iceberg.connect.events.LastDMLInfo;
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
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RecordUtils.class);
  private static final String DDL_CHECK_KEY = "is_ddl";
  public static final String POS_KEY = "pos";
  public static final String ROW_KEY = "row";
  public static final String GTID_KEY = "gtid";

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
    List<String> idCols = config.tableConfig(tableReference.identifier().name()).idColumns();
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

    TaskWriter<Record> writer;
    boolean isCdcEnabled =
        (config.tablesCdcField() != null && !config.tablesCdcField().isEmpty())
            || config.isUpsertMode();
    if (!isCdcEnabled) {
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
    } else {

      // DV enabled for table format version >=3
      boolean useDv;
      switch (TableUtil.formatVersion(table)) {
        case 1:
          throw new IllegalArgumentException(
              "CDC and upsert modes are not supported for Iceberg table format version 1");
        case 2:
          LOG.warn(
              "Table {} format version 2 detected. Delete Vectors are disabled. "
                  + "CDC and upsert modes work best with format version 3 or higher. "
                  + "Consider upgrading the table.",
              tableReference.identifier());
          useDv = false;
          break;
        default:
          useDv = true;
          break;
      }

      if (table.spec().isUnpartitioned()) {
        writer =
            new UnpartitionedDeltaWriter(
                table.spec(),
                format,
                writerFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                table.schema(),
                identifierFieldIds,
                config.isUpsertMode(),
                useDv,
                config.tablesCdcField());
      } else {
        writer =
            new PartitionedDeltaWriter(
                table.spec(),
                format,
                writerFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                table.schema(),
                identifierFieldIds,
                config.isUpsertMode(),
                useDv,
                config.tablesCdcField());
      }
    }
    return writer;
  }

  private RecordUtils() {}

  /**
   * DDL 처리 전, DML 들을 빠짐없이 처리 했는지 확인한다. (단일 파티션 검사)
   *
   * @param lastDMLInfo Kafka DDL message header 에서 추출된 "이전 버전의 마지막" gtid, pos, row 정보
   * @param ddlVersion 현재의 DDL 버전
   * @param status CMDB 로 부터 획득한 DML 처리 현황
   * @return DDL 메세지 헤더에 기록된 gtid, pos, row 정보와 CMDB 에 기록된 처리 내역이 일치 할 경우 true
   */
  public static boolean isPartitionDMLProcessed(
      LastDMLInfo lastDMLInfo, long ddlVersion, DmlStatusResponse status) {
    if (lastDMLInfo == null) {
      throw new IllegalStateException("lastDMLInfo must not be null");
    }

    if (status == null) {
      LOG.warn(
          "[DDL connector] No partition status found for partition: {}",
          lastDMLInfo.getPartition());
      return false;
    }

    if (ddlVersion == CmdbManager.UNDEFINED_DDL_VERSION) {
      LOG.info("[DDL connector] No last DML info found in DDL event and properties");
      LOG.info("[DDL connector] probably a new table, so no need to check");

      return true;
    }

    Integer ddlPartition = lastDMLInfo.getPartition();
    Long ddlPos = lastDMLInfo.getPos();
    Integer ddlRow = lastDMLInfo.getRow();
    String ddlGtid = lastDMLInfo.getGtId();

    DmlInfo dmlInfo = DmlInfo.fromJson(status.getLastProcessedDmlId());
    Long pos = Long.parseLong(dmlInfo.get(POS_KEY));
    Integer row = Integer.parseInt(dmlInfo.get(ROW_KEY));
    String gtid = dmlInfo.get(GTID_KEY);

    LOG.info(
        "[DDL connector] DDLReady DML Info, Partition: {}, Pos: {}, Row: {}, Gtid: {}",
        ddlPartition,
        ddlPos,
        ddlRow,
        ddlGtid);
    LOG.info(
        "[DDL connector] CMDB response DML Info, Partition: {}, Pos: {}, Row: {}, Gtid: {}",
        ddlPartition,
        pos,
        row,
        gtid);

    boolean isEligibleToProcess =
        (Objects.equals(ddlPos, pos) && Objects.equals(ddlRow, row) && ddlGtid.equals(gtid));
    LOG.info("[DDL connector] isEligibleToProcess: {}", isEligibleToProcess);

    return isEligibleToProcess;
  }

  public static boolean isDDLMessage(SinkRecord record) {
    for (Header header : record.headers()) {
      if (header.key().equals(DDL_CHECK_KEY)) {
        return headerValueToString(header.value()).equalsIgnoreCase("true");
      }
    }

    return false;
  }

  public static Long ddlVersionFromHeader(SinkRecord record) {
    Header ddlVersionHeader = record.headers().lastWithName("ddl_version");
    if (ddlVersionHeader == null || ddlVersionHeader.value() == null) {
      LOG.error("[DDL connector] Missing or null 'ddl_version' header in the record.");
      throw new RuntimeException("Missing or null 'ddl_version' header in the record.");
    }
    return Long.parseLong(headerValueToString(ddlVersionHeader.value()));
  }

  /**
   * Converts a header value to a string. Header values can be byte arrays or other types. This
   * method handles the conversion properly.
   *
   * @param value the header value
   * @return the string representation of the value
   */
  public static String headerValueToString(Object value) {
    if (value instanceof byte[]) {
      return new String((byte[]) value, StandardCharsets.UTF_8);
    }
    return value.toString();
  }
}
