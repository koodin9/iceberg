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
package org.apache.iceberg.connect.cmdb.dto.ddl.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.connect.cmdb.dto.common.DtoBase;
import org.apache.iceberg.connect.cmdb.model.DdlStatus;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DdlExecutionResponse extends DtoBase {

  @JsonProperty("id")
  private final long id;

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty("table_name")
  private String tableName;

  @JsonProperty("ddl_version")
  private final long ddlVersion;

  @JsonProperty("original_ddl")
  private final String originalDdl;

  @JsonProperty("translated_ddl")
  private final String translatedDdl;

  @JsonProperty("status")
  private final DdlStatus status;

  @JsonProperty("result_msg")
  private final String resultMsg;

  @JsonProperty("sink_schema")
  private final String sinkSchema;

  @JsonProperty("created_at")
  private final String createdAt;

  @JsonProperty("updated_at")
  private final String updatedAt;

  // Constructor
  public DdlExecutionResponse(
      @JsonProperty("id") long id,
      @JsonProperty("cluster_name") String clusterName,
      @JsonProperty("table_name") String tableName,
      @JsonProperty("ddl_version") long ddlVersion,
      @JsonProperty("original_ddl") String originalDdl,
      @JsonProperty("translated_ddl") String translatedDdl,
      @JsonProperty("status") DdlStatus status,
      @JsonProperty("result_msg") String resultMsg,
      @JsonProperty("sink_schema") String sinkSchema,
      @JsonProperty("created_at") String createdAt,
      @JsonProperty("updated_at") String updatedAt) {
    this.id = id;
    this.clusterName = clusterName;
    this.tableName = tableName;
    this.ddlVersion = ddlVersion;
    this.originalDdl = originalDdl;
    this.translatedDdl = translatedDdl;
    this.status = status;
    this.resultMsg = resultMsg;
    this.sinkSchema = sinkSchema;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
  }

  // Getters
  public long getId() {
    return id;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getTableName() {
    return tableName;
  }

  public long getDdlVersion() {
    return ddlVersion;
  }

  public String getOriginalDdl() {
    return originalDdl;
  }

  public String getTranslatedDdl() {
    return translatedDdl;
  }

  public DdlStatus getStatus() {
    return status;
  }

  public String getResultMsg() {
    return resultMsg;
  }

  public String getSinkSchema() {
    return sinkSchema;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public String getUpdatedAt() {
    return updatedAt;
  }
}
