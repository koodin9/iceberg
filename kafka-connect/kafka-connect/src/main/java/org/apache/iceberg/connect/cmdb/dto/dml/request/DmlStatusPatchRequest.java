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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.connect.cmdb.dto.common.DtoBase;
import org.apache.iceberg.connect.cmdb.model.PartitionStatus;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DmlStatusPatchRequest extends DtoBase {

  @JsonProperty("offset")
  private final Long offset;

  @JsonProperty("last_processed_dml_id")
  private final String lastProcessedDmlId;

  @JsonProperty("status")
  private final PartitionStatus status;

  // Constructor
  public DmlStatusPatchRequest(
      @JsonProperty("offset") Long offset,
      @JsonProperty("last_processed_dml_id") String lastProcessedDmlId,
      @JsonProperty("status") PartitionStatus status) {
    this.offset = offset;
    this.lastProcessedDmlId = lastProcessedDmlId;
    this.status = status;
  }

  // Getters
  public Long getOffset() {
    return offset;
  }

  public String getLastProcessedDmlId() {
    return lastProcessedDmlId;
  }

  public PartitionStatus getStatus() {
    return status;
  }
}
