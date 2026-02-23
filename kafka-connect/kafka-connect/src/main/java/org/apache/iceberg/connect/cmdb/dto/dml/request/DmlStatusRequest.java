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
package org.apache.iceberg.connect.cmdb.dto.dml.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.connect.cmdb.dto.common.DtoBase;
import org.apache.iceberg.connect.cmdb.model.PartitionStatus;

public class DmlStatusRequest extends DtoBase {

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty("table_name")
  private final String tableName;

  @JsonProperty("topic_name")
  private final String topicName;

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

  // Constructor
  public DmlStatusRequest(
      @JsonProperty("cluster_name") String clusterName,
      @JsonProperty("table_name") String tableName,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partition_id") long partitionId,
      @JsonProperty("offset") long offset,
      @JsonProperty("ddl_version") long ddlVersion,
      @JsonProperty("last_processed_dml_id") String lastProcessedDmlId,
      @JsonProperty("status") PartitionStatus status) {
    // 각 필드별로 validation 메서드 호출
    validateClusterName(clusterName);
    validateTableName(tableName);
    validateTopicName(topicName);
    validatePartitionId(partitionId);
    validateOffset(offset);
    validateDdlVersion(ddlVersion);
    validateLastProcessedDmlId(lastProcessedDmlId);
    validateStatus(status);

    this.clusterName = clusterName;
    this.tableName = tableName;
    this.topicName = topicName;
    this.partitionId = partitionId;
    this.offset = offset;
    this.ddlVersion = ddlVersion;
    this.lastProcessedDmlId = lastProcessedDmlId;
    this.status = status;
  }

  private static void validateClusterName(String clusterName) {
    if (clusterName == null || clusterName.isEmpty()) {
      throw new IllegalArgumentException("Cluster name cannot be null or empty");
    }
  }

  private static void validateTableName(String tableName) {
    if (tableName == null || tableName.isEmpty()) {
      throw new IllegalArgumentException("Table name cannot be null or empty");
    }
  }

  private static void validateTopicName(String topicName) {
    if (topicName == null || topicName.isEmpty()) {
      throw new IllegalArgumentException("Topic name cannot be null or empty");
    }
  }

  private static void validatePartitionId(long partitionId) {
    if (partitionId < 0) {
      throw new IllegalArgumentException("Partition ID cannot be negative");
    }
  }

  private static void validateOffset(long offset) {
    if (offset < -1) {
      throw new IllegalArgumentException("Offset cannot be less than -1");
    }
  }

  private static void validateDdlVersion(long ddlVersion) {
    if (ddlVersion < -1) {
      throw new IllegalArgumentException("DDL version cannot be less than -1");
    }
  }

  private static void validateLastProcessedDmlId(String lastProcessedDmlId) {
    if (lastProcessedDmlId == null || lastProcessedDmlId.isEmpty()) {
      throw new IllegalArgumentException("Last processed DML ID cannot be null or empty");
    }
  }

  private static void validateStatus(PartitionStatus status) {
    if (status == null) {
      throw new IllegalArgumentException("Status cannot be null");
    }
  }

  // Getters
  public String getClusterName() {
    return clusterName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTopicName() {
    return topicName;
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
}
