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
package org.apache.iceberg.connect.events;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/**
 * DDL 레코드에서 마지막으로 처리된 DML GTID 정보를 담는 클래스다.
 *
 * <p>이 클래스는 특정 파티션에서 DDL 적용 전에 마지막으로 처리해야 하는 DML 작업의 위치 정보를 저장한다. DDL 적용 시 데이터 일관성 유지를 위해 각 파티션별로
 * 어디까지 DML이 처리되어야 하는지 추적한다.
 *
 * <p>gtId, 위치 정보(pos), 행 번호(row) 등을 포함하여 정확한 트랜잭션 위치를 식별할 수 있다. 이 정보는 DDLReady 이벤트의 일부로 전송되어
 * Coordinator가 DDL 적용 전 모든 작업이 완료되었는지 확인하는 데 사용된다.
 */
public class LastDMLInfo implements IndexedRecord {

  /** 이 DML 정보가 속한 파티션 번호다. */
  private Integer partition;

  /** MySQL의 Global Transaction ID 값이다. 트랜잭션을 고유하게 식별한다. */
  private String gtId;

  /** 바이너리 로그에서의 위치 값이다. 특정 트랜잭션 내에서의 정확한 위치를 나타낸다. */
  private Long pos;

  /** 해당 위치 내에서의 행 번호다. 같은 위치에 여러 행이 있는 경우 정확한 행을 식별한다. */
  private Integer row;

  /** 직렬화 및 역직렬화를 위한 이 레코드와 연결된 Avro 스키마다. */
  private final Schema avroSchema;

  /** Iceberg 스키마에서 필드 ID 상수다. 직렬화 및 역직렬화에 사용된다. */
  static final int PARTITION = 10_900;

  static final int GTID = 10_901;
  static final int POS = 10_902;
  static final int ROW = 10_903;

  /** LastDMLInfo 클래스의 Iceberg 스키마 정의다. */
  public static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(PARTITION, "partition", IntegerType.get()),
          NestedField.required(GTID, "gtid", StringType.get()),
          NestedField.optional(POS, "pos", LongType.get()),
          NestedField.optional(ROW, "row", IntegerType.get()));

  /** Iceberg 스키마에서 변환된 Avro 스키마다. 직렬화 및 역직렬화에 사용된다. */
  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA, LastDMLInfo.class);

  // Used by Avro reflection to instantiate this class when reading events
  public LastDMLInfo(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public LastDMLInfo(Integer partition, String gtId, Long pos, Integer row) {
    this.partition = partition;
    this.gtId = gtId;
    this.pos = pos;
    this.row = row;
    this.avroSchema = AVRO_SCHEMA;
  }

  public Integer getPartition() {
    return partition;
  }

  public String getGtId() {
    return gtId;
  }

  public Long getPos() {
    return pos;
  }

  public Integer getRow() {
    return row;
  }

  @Override
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case PARTITION:
        partition = (Integer) v;
        break;
      case GTID:
        gtId = v.toString();
        break;
      case POS:
        pos = (Long) v;
        break;
      case ROW:
        row = (Integer) v;
        break;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    return switch (AvroUtil.positionToId(i, avroSchema)) {
      case PARTITION -> partition;
      case GTID -> gtId;
      case POS -> pos;
      case ROW -> row;
      default -> throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    };
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }
}
