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
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
 * DDL 작업 완료를 나타내는 이벤트 페이로드 클래스다.
 *
 * <p>이 클래스는 {@link Payload} 인터페이스를 구현하며, DDL(Data Definition Language) 작업이 성공적으로 실행되었을 때 발행되는 이벤트를
 * 나타낸다. 이 이벤트는 적용된 DDL 버전 정보를 포함하고 있으며, 이 정보는 추적 및 동기화 목적으로 사용된다.
 *
 * <p>DDLComplete 이벤트는 Coordinator에서 발행되며 Worker에서 수신된다. Worker는 이 이벤트를 수신하여 DDL 작업이 성공적으로 완료되었음을
 * 확인하고, Paused 상태인 파티션들에 대해 resume 작업을 수행한다.
 *
 * <p>페이로드 구조는 간단하며, DDL 버전 식별자만 포함하고 있다.
 */
public class DDLComplete implements Payload {
  /** 완료된 DDL 작업의 버전 식별자. 선행 DDLReady 이벤트에서 사용된 버전과 동일 */
  private Long ddlVersion;

  private final Schema avroSchema;

  /** Iceberg 스키마에서 ddl_version 필드의 필드 ID 상수. 스키마 진화 및 직렬화에 사용 */
  static final int DDL_VERSION = 11_000;

  private static final StructType ICEBERG_SCHEMA =
      StructType.of(NestedField.required(DDL_VERSION, "ddl_version", LongType.get()));

  /** Iceberg 스키마에서 변환된 Avro 스키마. 전송을 위한 페이로드 인스턴스의 직렬화 및 역직렬화에 사용된다. */
  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA, DDLComplete.class);

  /**
   * 이벤트 읽기 시 Avro 리플렉션에 의해 이 클래스를 인스턴스화하는 데 사용되는 생성자다. 이 생성자는 Avro의 리플렉션 기반 역직렬화 프로세스에 필요하다.
   *
   * @param avroSchema 이 인스턴스에 사용할 Avro 스키마
   */
  // Used by Avro reflection to instantiate this class when reading events
  public DDLComplete(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public DDLComplete(long ddlVersion) {
    this.ddlVersion = ddlVersion;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.DDL_COMPLETE;
  }

  public long ddlVersion() {
    return ddlVersion;
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
      case DDL_VERSION:
        this.ddlVersion = (long) v;
        break;
      default:
        throw new IllegalArgumentException("Unknown field id: " + i);
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case DDL_VERSION:
        return this.ddlVersion;
      default:
        throw new IllegalArgumentException("Unknown field id: " + i);
    }
  }
}
