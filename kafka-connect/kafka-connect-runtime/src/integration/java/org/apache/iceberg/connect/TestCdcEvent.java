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
package org.apache.iceberg.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Date;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

/** CDC 테스트용 이벤트 클래스 - Debezium envelope 형식 사용 before/after 구조, source 메타데이터, header 포함 */
public class TestCdcEvent implements TestEventBase {
  // Iceberg 테이블 스키마용 source 타입 ($__source로 변환됨)
  public static final Types.StructType $__SOURCE =
      Types.StructType.of(
          Types.NestedField.required(7, "version", Types.StringType.get()),
          Types.NestedField.required(8, "connector", Types.StringType.get()),
          Types.NestedField.required(9, "name", Types.StringType.get()),
          Types.NestedField.required(10, "ts_ms", Types.LongType.get()),
          Types.NestedField.required(11, "db", Types.StringType.get()),
          Types.NestedField.required(12, "ts_us", Types.LongType.get()),
          Types.NestedField.required(13, "ts_ns", Types.LongType.get()),
          Types.NestedField.required(14, "table", Types.StringType.get()),
          Types.NestedField.required(15, "server_id", Types.IntegerType.get()),
          Types.NestedField.required(16, "gtid", Types.StringType.get()),
          Types.NestedField.required(17, "file", Types.StringType.get()),
          Types.NestedField.required(18, "pos", Types.LongType.get()),
          Types.NestedField.required(19, "row", Types.IntegerType.get()),
          Types.NestedField.required(20, "thread", Types.LongType.get()));

  // DebeziumTransform이 추가하는 _cdc 메타데이터 타입
  public static final Types.StructType $_CDC =
      Types.StructType.of(
          Types.NestedField.required(21, "op", Types.StringType.get()),
          Types.NestedField.required(22, "ts", Types.TimestampType.withZone()),
          Types.NestedField.optional(23, "offset", Types.LongType.get()),
          Types.NestedField.required(24, "source", Types.StringType.get()),
          Types.NestedField.required(25, "target", Types.StringType.get()));

  // Iceberg 테이블 스키마 - DebeziumTransform이 변환한 후의 최종 스키마
  public static final Schema TEST_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "type", Types.StringType.get()),
              Types.NestedField.required(3, "data", Types.StringType.get()),
              Types.NestedField.required(4, "ts", Types.TimestampType.withZone()),
              Types.NestedField.required(5, "payload", Types.StringType.get()),
              Types.NestedField.required(6, "$__source", $__SOURCE),
              Types.NestedField.required(26, "_cdc", $_CDC)),
          ImmutableSet.of(1));

  public static final Schema TEST_SCHEMA_NO_ID =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "type", Types.StringType.get()),
              Types.NestedField.required(3, "data", Types.StringType.get()),
              Types.NestedField.required(4, "ts", Types.TimestampType.withZone()),
              Types.NestedField.required(5, "payload", Types.StringType.get()),
              Types.NestedField.required(6, "$__source", $__SOURCE),
              Types.NestedField.required(26, "_cdc", $_CDC)));

  public static final org.apache.kafka.connect.data.Schema HEADER_SCHEMA =
      SchemaBuilder.struct()
          .name("header")
          .field("dbms_type", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("is_ddl", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ddl_version", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("dml_info", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .build();

  // Debezium source 메타데이터 스키마
  public static final org.apache.kafka.connect.data.Schema SOURCE_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .name("source")
          .field("version", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("connector", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ts_ms", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("db", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ts_us", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("ts_ns", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("table", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("server_id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
          .field("gtid", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("file", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("pos", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("row", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
          .field("thread", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .build();

  // Debezium before/after payload 스키마 (실제 테이블 데이터)
  // optional: INSERT시 before=null, DELETE시 after=null
  public static final org.apache.kafka.connect.data.Schema PAYLOAD_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .name("payload")
          .optional()
          .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("type", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("data", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ts", Timestamp.SCHEMA)
          .field("payload", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .build();

  // Debezium envelope 스키마 (before, after, source, op, ts_ms)
  public static final org.apache.kafka.connect.data.Schema TEST_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .name("envelope")
          .field("before", PAYLOAD_CONNECT_SCHEMA)
          .field("after", PAYLOAD_CONNECT_SCHEMA)
          .field("source", SOURCE_CONNECT_SCHEMA)
          .field("op", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ts_ms", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("header", HEADER_SCHEMA)
          .build();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final JsonConverter JSON_CONVERTER = new JsonConverter();

  static {
    JSON_CONVERTER.configure(
        ImmutableMap.of(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName()));
  }

  public static final PartitionSpec TEST_SPEC =
      PartitionSpec.builderFor(TEST_SCHEMA).day("ts").build();

  private final long id;
  private final String type;
  private final String data;
  private final Instant ts;
  private final String payload;
  private final String op; // Debezium op: c(create), u(update), d(delete)
  private final Struct source;
  private final Struct header;

  public TestCdcEvent(
      long id, String type, String data, Instant ts, String payload, Struct source, Struct header) {
    this(id, type, data, ts, payload, "c", source, header);
  }

  public TestCdcEvent(
      long id,
      String type,
      String data,
      Instant ts,
      String payload,
      String op,
      Struct source,
      Struct header) {
    this.id = id;
    this.type = type;
    this.data = data;
    this.ts = ts;
    this.payload = payload;
    this.op = op;
    this.source = source;
    this.header = header;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public String serialize(boolean useSchema) {
    try {
      // before/after에 들어갈 실제 테이블 데이터
      Struct payloadStruct =
          new Struct(PAYLOAD_CONNECT_SCHEMA)
              .put("id", id)
              .put("type", type)
              .put("data", data)
              .put("ts", Date.from(ts))
              .put("payload", payload);

      // Debezium envelope 구조
      Struct envelope =
          new Struct(TEST_CONNECT_SCHEMA)
              .put("source", source)
              .put("op", op)
              .put("ts_ms", ts.toEpochMilli())
              .put("header", header);

      // op에 따라 before/after 설정
      if ("d".equals(op)) {
        // DELETE: before에 데이터, after는 null
        envelope.put("before", payloadStruct);
        envelope.put("after", null);
      } else {
        // INSERT/UPDATE: after에 데이터, before는 null
        envelope.put("before", null);
        envelope.put("after", payloadStruct);
      }

      String convertMethod =
          useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
      JsonNode json =
          DynMethods.builder(convertMethod)
              .hiddenImpl(
                  JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
              .build(JSON_CONVERTER)
              .invoke(TEST_CONNECT_SCHEMA, envelope);
      return MAPPER.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Struct header() {
    return this.header;
  }

  /** DML 헤더 생성 헬퍼 메서드 */
  public static Struct createDmlHeader(String gtid, String pos, String row, long ddlVersion) {
    return new Struct(HEADER_SCHEMA)
        .put("dbms_type", "mysql")
        .put("is_ddl", "false")
        .put("ddl_version", String.valueOf(ddlVersion))
        .put(
            "dml_info",
            String.format("{\"gtid\":\"%s\",\"pos\":\"%s\",\"row\":\"%s\"}", gtid, pos, row));
  }

  /** 기본 DML 헤더 생성 (ddlVersion = 1) */
  public static Struct createDmlHeader(String gtid, String pos, String row) {
    return createDmlHeader(gtid, pos, row, 1L);
  }

  /** Debezium source 메타데이터 Struct 생성 헬퍼 메서드 */
  public static Struct createSourceStruct() {
    Struct sourceStruct = new Struct(SOURCE_CONNECT_SCHEMA);
    sourceStruct.put("version", "2.7.0.Final");
    sourceStruct.put("connector", "mysql");
    sourceStruct.put("name", "my-12345-00-koodin-koodin-icebergdemo1");
    sourceStruct.put("ts_ms", 1731303538282L);
    sourceStruct.put("db", "cdcdb");
    sourceStruct.put("ts_us", 1731303538282308L);
    sourceStruct.put("ts_ns", 1731303538282308000L);
    sourceStruct.put("table", "icebergdemo1");
    sourceStruct.put("server_id", 1);
    sourceStruct.put("gtid", "8421ee9f-8fdf-11ed-91e8-fa163ef8d756:240094");
    sourceStruct.put("file", "binlog.000007");
    sourceStruct.put("pos", 81836861L);
    sourceStruct.put("row", 0);
    sourceStruct.put("thread", 37132L);
    return sourceStruct;
  }
}
