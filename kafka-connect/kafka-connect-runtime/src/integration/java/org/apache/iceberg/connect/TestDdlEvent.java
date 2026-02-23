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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
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

/** DDL 이벤트 테스트용 클래스 - 실제 Debezium DDL 메시지 형식에 맞춤 */
public class TestDdlEvent implements TestEventBase {
  public static final Types.StructType $__SOURCE =
      Types.StructType.of(
          Types.NestedField.required(8, "version", Types.StringType.get()),
          Types.NestedField.required(9, "connector", Types.StringType.get()),
          Types.NestedField.required(10, "name", Types.StringType.get()),
          Types.NestedField.required(11, "ts_ms", Types.LongType.get()),
          Types.NestedField.required(12, "db", Types.StringType.get()),
          Types.NestedField.required(13, "ts_us", Types.LongType.get()),
          Types.NestedField.required(14, "ts_ns", Types.LongType.get()),
          Types.NestedField.required(15, "table", Types.StringType.get()),
          Types.NestedField.required(16, "server_id", Types.IntegerType.get()),
          Types.NestedField.required(17, "gtid", Types.StringType.get()),
          Types.NestedField.required(18, "file", Types.StringType.get()),
          Types.NestedField.required(19, "pos", Types.LongType.get()),
          Types.NestedField.required(20, "row", Types.IntegerType.get()),
          Types.NestedField.required(21, "thread", Types.LongType.get()));

  public static final org.apache.iceberg.Schema TEST_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "ts_ms", Types.TimestampType.withZone()),
              Types.NestedField.required(3, "databaseName", Types.StringType.get()),
              Types.NestedField.required(4, "ddl", Types.StringType.get()),
              Types.NestedField.required(
                  5, "tableChanges", Types.ListType.ofRequired(6, Types.StringType.get())),
              Types.NestedField.required(7, "$__source", $__SOURCE)),
          ImmutableSet.of(1));

  public static final org.apache.iceberg.Schema TEST_SCHEMA_NO_ID =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "ts_ms", Types.TimestampType.withZone()),
              Types.NestedField.required(3, "databaseName", Types.StringType.get()),
              Types.NestedField.required(4, "ddl", Types.StringType.get()),
              Types.NestedField.required(
                  5, "tableChanges", Types.ListType.ofRequired(6, Types.StringType.get())),
              Types.NestedField.required(7, "$__source", $__SOURCE)));

  public static final org.apache.kafka.connect.data.Schema HEADER_SCHEMA =
      SchemaBuilder.struct()
          .name("header")
          .field("last_dml_info", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("dbms_type", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("is_ddl", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ddl_version", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .build();

  public static final org.apache.kafka.connect.data.Schema TEST_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("ts_ms", Timestamp.SCHEMA)
          .field("databaseName", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ddl", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field(
              "tableChanges",
              SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build())
          .field(
              "$__source",
              SchemaBuilder.struct()
                  .name("$__source")
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
                  .build())
          .field("op", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
          .field("header", HEADER_SCHEMA);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final JsonConverter JSON_CONVERTER = new JsonConverter();

  static {
    JSON_CONVERTER.configure(
        ImmutableMap.of(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName()));
  }

  private final long id;
  private final Instant tsMs;
  private final String databaseName;
  private final String tableName;
  private final String ddl;
  private final Struct header;

  public TestDdlEvent(
      long id, Instant tsMs, String databaseName, String tableName, String ddl, Struct header) {
    this.id = id;
    this.tsMs = tsMs;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.ddl = ddl;
    this.header = header;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Struct header() {
    return header;
  }

  @Override
  public String serialize(boolean useSchema) {
    try {
      if (useSchema) {
        // useSchema=true: JsonConverter를 사용하여 schema envelope 형식으로 직렬화
        org.apache.kafka.connect.data.Schema sourceSchema =
            TEST_CONNECT_SCHEMA.field("$__source").schema();

        Struct sourceStruct =
            new Struct(sourceSchema)
                .put("version", "3.0.0.Final")
                .put("connector", "mysql")
                .put("name", "test-connector")
                .put("ts_ms", tsMs.toEpochMilli())
                .put("db", databaseName)
                .put("ts_us", tsMs.toEpochMilli() * 1000)
                .put("ts_ns", tsMs.toEpochMilli() * 1000000)
                .put("table", tableName != null ? tableName : "test_table")
                .put("server_id", 1)
                .put("gtid", "325bd1dc-1104-11f0-9f65-e43d1a7e41a0:1")
                .put("file", "mysql-bin.000001")
                .put("pos", 12345L)
                .put("row", 0)
                .put("thread", 0L);

        Struct envelope =
            new Struct(TEST_CONNECT_SCHEMA)
                .put("id", id)
                .put("ts_ms", Date.from(tsMs))
                .put("databaseName", databaseName)
                .put("ddl", ddl)
                .put("tableChanges", Collections.emptyList())
                .put("$__source", sourceStruct)
                .put("op", null)
                .put("header", header);

        JsonNode json =
            DynMethods.builder("convertToJsonWithEnvelope")
                .hiddenImpl(
                    JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
                .build(JSON_CONVERTER)
                .invoke(TEST_CONNECT_SCHEMA, envelope);
        return MAPPER.writeValueAsString(json);
      } else {
        // useSchema=false: plain JSON 형식으로 직렬화
        ObjectNode root = MAPPER.createObjectNode();

        // source 객체
        ObjectNode source = MAPPER.createObjectNode();
        source.put("version", "3.0.0.Final");
        source.put("connector", "mysql");
        source.put("name", "test-connector");
        source.put("ts_ms", tsMs.toEpochMilli());
        source.putNull("snapshot");
        source.put("db", databaseName);
        source.putNull("sequence");
        source.putObject("ts_us").put("long", tsMs.toEpochMilli() * 1000);
        source.putObject("ts_ns").put("long", tsMs.toEpochMilli() * 1000000);
        source.putObject("table").put("string", tableName != null ? tableName : "test_table");
        source.put("server_id", 1);
        source.putObject("gtid").put("string", "325bd1dc-1104-11f0-9f65-e43d1a7e41a0:1");
        source.put("file", "mysql-bin.000001");
        source.put("pos", 12345);
        source.put("row", 0);
        source.putNull("thread");
        source.putNull("query");

        root.set("source", source);
        root.put("ts_ms", tsMs.toEpochMilli());
        root.putObject("databaseName").put("string", databaseName);
        root.putNull("schemaName");
        root.putObject("ddl").put("string", ddl);

        // tableChanges 배열 (빈 배열로 설정)
        ArrayNode tableChanges = MAPPER.createArrayNode();
        root.set("tableChanges", tableChanges);

        return MAPPER.writeValueAsString(root);
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /** DDL 헤더 생성 헬퍼 메서드 */
  public static Struct createDdlHeader(String gtid, String pos, String row, long ddlVersion) {
    return new Struct(HEADER_SCHEMA)
        .put(
            "last_dml_info",
            String.format(
                "{\"0\":{\"gtid\":\"%s\",\"pos\":\"%s\",\"row\":\"%s\"}}", gtid, pos, row))
        .put("dbms_type", "mysql")
        .put("is_ddl", "true")
        .put("ddl_version", String.valueOf(ddlVersion));
  }

  /** 기본 DDL 헤더 생성 (ddlVersion = 2) */
  public static Struct createDdlHeader(String gtid, String pos, String row) {
    return createDdlHeader(gtid, pos, row, 2L);
  }
}
