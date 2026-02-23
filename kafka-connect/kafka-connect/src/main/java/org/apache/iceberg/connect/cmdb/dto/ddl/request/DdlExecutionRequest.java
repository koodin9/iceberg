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
package org.apache.iceberg.connect.cmdb.dto.ddl.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.connect.cmdb.dto.common.DtoBase;
import org.apache.iceberg.connect.cmdb.model.DdlStatus;

/** DDL 메세지 수신시 신규 ddl execution 등록에 사용 */
public class DdlExecutionRequest extends DtoBase {

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty("table_name")
  private final String tableName;

  @JsonProperty("ddl_version")
  private final long ddlVersion;

  @JsonProperty("original_ddl")
  private final String originalDdl;

  @JsonProperty("translated_ddl")
  private final String translatedDdl;

  @JsonProperty("status")
  private final DdlStatus ddlStatus;

  @JsonProperty("result_msg")
  private final String resultMsg;

  @JsonProperty("sink_schema")
  private final String sinkSchema;

  /** JSON 역직렬화용 생성자 - 모든 필드를 받음 */
  @JsonCreator
  public DdlExecutionRequest(
      @JsonProperty("cluster_name") String clusterName,
      @JsonProperty("table_name") String tableName,
      @JsonProperty("ddl_version") long ddlVersion,
      @JsonProperty("original_ddl") String originalDdl,
      @JsonProperty("translated_ddl") String translatedDdl,
      @JsonProperty("status") DdlStatus ddlStatus,
      @JsonProperty("result_msg") String resultMsg,
      @JsonProperty("sink_schema") String sinkSchema) {
    if (clusterName == null || clusterName.isEmpty()) {
      throw new IllegalArgumentException("Cluster Name cannot be null or empty");
    }
    if (tableName == null || tableName.isEmpty()) {
      throw new IllegalArgumentException("Table name cannot be null or empty");
    }
    if (ddlVersion < 0) {
      throw new IllegalArgumentException("DDL version cannot be negative");
    }

    this.clusterName = clusterName;
    this.tableName = tableName;
    this.ddlVersion = ddlVersion;
    this.originalDdl = originalDdl;
    this.translatedDdl = translatedDdl;
    this.ddlStatus = ddlStatus != null ? ddlStatus : DdlStatus.RECEIVED;
    this.resultMsg = resultMsg;
    this.sinkSchema = sinkSchema;
  }

  /** DDL 메세지 수신시 신규 등록용 - originalDdl 필수 */
  public DdlExecutionRequest(
      String clusterName, String tableName, long ddlVersion, String originalDdl) {
    this(clusterName, tableName, ddlVersion, originalDdl, null, DdlStatus.RECEIVED, null, null);
    if (originalDdl == null || originalDdl.isEmpty()) {
      throw new IllegalArgumentException("Original DDL cannot be null or empty");
    }
  }

  /** 초기 상태 기록용. 처음부터 status 를 Processed 로 등록한다. */
  public DdlExecutionRequest(String clusterName, String tableName, long ddlVersion) {
    this(
        clusterName,
        tableName,
        ddlVersion,
        null,
        null,
        DdlStatus.PROCESSED,
        "CDC init marker - process started with DML",
        null);
  }

  // Getters
  public long getDdlVersion() {
    return ddlVersion;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getOriginalDdl() {
    return originalDdl;
  }

  public String getTranslatedDdl() {
    return translatedDdl;
  }

  public DdlStatus getStatus() {
    return ddlStatus;
  }

  public String getResultMsg() {
    return resultMsg;
  }

  public String getSinkSchema() {
    return sinkSchema;
  }
}
