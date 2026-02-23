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
package org.apache.iceberg.connect.cmdb.dto.dml.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.connect.cmdb.dto.common.DtoBase;
import org.apache.iceberg.connect.cmdb.model.PartitionStatus;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DmlStatusResponse extends DtoBase {

  @JsonProperty("id")
  private final long id;

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty("table_name")
  private final String tableName;

  @JsonProperty("partition_id")
  private final long partitionId;

  @JsonProperty("offset")
  private final long offset;

  @JsonProperty("ddl_version")
  private final long ddlVersion;

  @JsonProperty("last_processed_dml_id")
  private final String lastProcessedDmlId;

  @JsonProperty("status")
  private final PartitionStatus status;

  @JsonProperty("created_at")
  private final String createdAt;

  @JsonProperty("updated_at")
  private final String updatedAt;

  // Constructor
  public DmlStatusResponse(
      @JsonProperty("id") long id,
      @JsonProperty("cluster_name") String clusterName,
      @JsonProperty("table_name") String tableName,
      @JsonProperty("partition_id") long partitionId,
      @JsonProperty("offset") long offset,
      @JsonProperty("ddl_version") long ddlVersion,
      @JsonProperty("last_processed_dml_id") String lastProcessedDmlId,
      @JsonProperty("status") PartitionStatus status,
      @JsonProperty("created_at") String createdAt,
      @JsonProperty("updated_at") String updatedAt) {
    this.id = id;
    this.clusterName = clusterName;
    this.tableName = tableName;
    this.partitionId = partitionId;
    this.offset = offset;
    this.ddlVersion = ddlVersion;
    this.lastProcessedDmlId = lastProcessedDmlId;
    this.status = status;
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

  public long getPartitionId() {
    return partitionId;
  }

  public long getOffset() {
    return offset;
  }

  public long getDdlVersion() {
    return ddlVersion;
  }

  public String getLastProcessedDmlId() {
    return lastProcessedDmlId;
  }

  public PartitionStatus getStatus() {
    return status;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public String getUpdatedAt() {
    return updatedAt;
  }
}
