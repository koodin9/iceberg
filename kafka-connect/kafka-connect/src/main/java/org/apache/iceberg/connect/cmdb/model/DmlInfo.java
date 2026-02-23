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
package org.apache.iceberg.connect.cmdb.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * DmlInfo 클래스는 Sink Connector에서 DDL을 Sink DB에 적용하기 위해 각 파티션의 마지막 DML 정보를 추상화하여 관리하는 역할을 수행한다.
 *
 * <p># 배경 - DDL(v3)을 Sink DB에 적용하기 전에, 해당 DDL 이전에 발생한 각 파티션의 마지막 DML 처리가 완료되었는지 확인해야 한다.
 *
 * <p># MySQL 기준 DML 식별 정보 - MySQL에서는 마지막 DML을 식별하는 정보로 gtid, pos, row를 사용한다. - 이 값들은 MySQL의 binlog에
 * 기록되는 트랜잭션 정보이기 때문에, Debezium Source Connector에서 메시지를 재발행하더라도 변하지 않는다.
 *
 * <p># DBMS에 따른 식별자 차이 - 다른 DBMS에서도 DDL을 지원하는 CDC 시스템을 적용하려면 DML 식별 정보가 필요하다. - DBMS마다 DML 식별자가 다를
 * 수 있다. 예: PostgreSQL의 경우 LSN(Log Sequence Number)
 *
 * <p># 통합 메시지 프로토콜 - DBMS마다 식별 정보가 다르더라도 이를 추상화하는 메시지 프로토콜을 정의하면, Source DBMS에 관계없이 Sink Connector는
 * 항상 동일하게 동작할 수 있다.
 *
 * <p># DML 메시지 예 아래는 DML 정보를 표현하는 JSON 예이다. { "is_ddl": "false", "ddl_version": "3", "dml_info": {
 * "gtid": "abc123", "pos": "456", "row": "10" } }
 *
 * <p>이 클래스는 다양한 DBMS의 DML 식별 정보를 통합된 형태로 관리하고, JSON과 Map을 통해 직렬화/역직렬화를 지원한다.
 *
 * <p><a href="https://wiki.daumkakao.com/pages/viewpage.action?pageId=1656635083">ZeroETL 카프카 메시지
 * 프로토콜</a>
 */
public record DmlInfo(Map<String, String> data) {

  public DmlInfo(Map<String, String> data) {
    this.data = data != null ? Maps.newHashMap(data) : Maps.newHashMap();
  }

  public String get(String key) {
    return data.getOrDefault(key, "");
  }

  public boolean isEmpty() {
    return data.isEmpty();
  }

  public String toJson() {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      ObjectNode jsonNode = objectMapper.createObjectNode();
      data.forEach(jsonNode::put);
      return objectMapper.writeValueAsString(jsonNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert DMLInfo to JSON", e);
    }
  }

  public static DmlInfo fromJson(String json) {
    try {
      TypeReference<Map<String, String>> typeRef = new TypeReference<>() {};
      ObjectMapper objectMapper = new ObjectMapper();
      Map<String, String> map = objectMapper.readValue(json, typeRef);

      return new DmlInfo(map);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse DMLInfo from JSON", e);
    }
  }

  public static DmlInfo fromMap(Map<?, ?> map) {
    if (map == null || map.isEmpty()) {
      return new DmlInfo(Maps.newHashMap());
    }

    Map<String, String> data = Maps.newHashMap();
    map.forEach(
        (key, value) -> {
          if (key instanceof String && value != null) {
            data.put((String) key, value.toString());
          }
        });

    return new DmlInfo(data);
  }

  @Override
  public String toString() {
    return "DMLInfo{" + "data=" + data + '}';
  }

  /**
   * 두 DMLInfo 객체를 비교하여 키와 값이 모두 같은지 확인한다. - 키의 집합이 동일한지 검사 - 각 키에 대해 값이 모두 일치하는지 검사
   *
   * @param obj 비교 대상 객체
   * @return 키와 값이 모두 같으면 true, 그렇지 않으면 false
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof DmlInfo other)) {
      return false;
    }

    // 1. 키 집합이 같은지 확인
    if (!data.keySet().equals(other.data.keySet())) {
      return false;
    }

    // 2. 키에 대응하는 값이 모두 같은지 확인
    for (String key : data.keySet()) {
      if (!Objects.equals(data.get(key), other.data.get(key))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(data);
  }

  public static DmlInfo empty() {
    return new DmlInfo(Maps.newHashMap());
  }
}
