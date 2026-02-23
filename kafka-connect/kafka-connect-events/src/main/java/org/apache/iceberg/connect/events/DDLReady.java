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

import java.util.List;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/**
 * DDL 작업 준비가 완료되었음을 나타내는 이벤트 페이로드 클래스다.
 *
 * <p>이 클래스는 {@link Payload} 인터페이스를 구현하며, 실행 준비가 된 DDL(Data Definition Language) 작업에 대한 이벤트를 나타낸다. 이
 * 이벤트는 실행될 DDL 문장, DDL 버전 정보, 그리고 DDL 적용을 위해 수행되어야 하는 마지막 DML 정보를 포함한다.
 *
 * <p>DDLReady 이벤트는 Worker에서 발행되어 Coordinator로 전송된다. Coordinator는 이 이벤트를 수신하면 해당 DDL을 적용하고, 작업이 완료된
 * 후 DDLComplete 이벤트를 발행한다.
 *
 * <p>테이블 스키마 변경이 필요한 경우, 관련 파티션들은 일시 중지(pause)되고 DDL 작업이 완료될 때까지 대기한다.
 */
public class DDLReady implements Payload {

  /** 실행할 DDL 문장. SQL 형태의 DDL 구문이 포함된다. */
  private String ddl;

  /** DDL 작업의 버전 식별자. */
  private Long ddlVersion;

  /**
   * DDL 적용을 위해 수행되어야 하는 마지막 DML 작업 정보 목록. 각 파티션별 마지막 DML 위치 정보를 포함한다. 이 정보는 worker 에서 이벤트 생성시 kafka
   * message header 의 정보를 추출해 작성된다.
   */
  private List<LastDMLInfo> lastDMLInfoList;

  /** 직렬화 및 역직렬화를 위한 이 페이로드와 연결된 Avro 스키마다. */
  private final Schema avroSchema;

  /** Iceberg 스키마에서 필드 ID 상수. 직렬화 및 역직렬화에 사용된다. */
  static final int DDL = 10_800;

  static final int DDL_VERSION = 10_801;
  static final int LAST_DML_INFO = 10_802;
  static final int LAST_DML_INFO_ELEMENT = 10_803;

  /** 이 페이로드의 Iceberg 스키마 정의다. */
  private static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(DDL, "ddl", StringType.get()),
          NestedField.required(DDL_VERSION, "ddl_version", LongType.get()),
          NestedField.required(
              LAST_DML_INFO,
              "last_dml_info",
              ListType.ofRequired(LAST_DML_INFO_ELEMENT, LastDMLInfo.ICEBERG_SCHEMA)));

  /** Iceberg 스키마에서 변환된 Avro 스키마다. 전송을 위한 페이로드 인스턴스의 직렬화 및 역직렬화에 사용된다. */
  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA, DDLReady.class);

  // Used by Avro reflection to instantiate this class when reading events
  public DDLReady(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public DDLReady(
      TableReference tableReference,
      String ddl,
      long ddlVersion,
      List<LastDMLInfo> lastDMLInfoList) {
    Preconditions.checkNotNull(tableReference, "Table reference cannot be null");
    this.ddl = ddl;
    this.ddlVersion = ddlVersion;
    this.lastDMLInfoList = lastDMLInfoList;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.DDL_READY;
  }

  public String ddl() {
    return ddl;
  }

  public long ddlVersion() {
    return ddlVersion;
  }

  public List<LastDMLInfo> lastDMLInfoList() {
    return lastDMLInfoList;
  }

  @Override
  public StructType writeSchema() {
    return ICEBERG_SCHEMA;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case DDL:
        this.ddl = v.toString();
        break;
      case DDL_VERSION:
        this.ddlVersion = (long) v;
        break;
      case LAST_DML_INFO:
        this.lastDMLInfoList = (List<LastDMLInfo>) v;
        break;
      default:
        throw new IllegalArgumentException("Unknown field id: " + i);
    }
  }

  @Override
  public Object get(int i) {
    return switch (AvroUtil.positionToId(i, avroSchema)) {
      case DDL -> this.ddl;
      case DDL_VERSION -> this.ddlVersion;
      case LAST_DML_INFO -> this.lastDMLInfoList;
      default -> throw new IllegalArgumentException("Unknown field id: " + i);
    };
  }
}
