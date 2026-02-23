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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.connect.cmdb.dto.common.DtoBase;
import org.apache.iceberg.connect.cmdb.model.DdlStatus;

/** DDL 적용 후 ddl execution 결과값 업데이트에 사용 */
public class DdlExecutionPatchRequest extends DtoBase {

  @JsonProperty("translated_ddl")
  private final String translatedDdl;

  @JsonProperty("status")
  private final DdlStatus ddlStatus;

  @JsonProperty("result_msg")
  private final String resultMsg;

  @JsonProperty("sink_schema")
  private final String sinkSchema;

  public DdlExecutionPatchRequest(
      @JsonProperty("translated_ddl") String translatedDdl,
      @JsonProperty("status") DdlStatus ddlStatus,
      @JsonProperty("result_msg") String resultMsg,
      @JsonProperty("sink_schema") String sinkSchema) {
    if (translatedDdl == null || translatedDdl.isEmpty()) {
      throw new IllegalArgumentException("Original DDL cannot be null or empty");
    }
    if (ddlStatus == null || ddlStatus == DdlStatus.RECEIVED) {
      throw new IllegalArgumentException(
          "Status cannot be null and cannot be updated to 'received'");
    }
    if (resultMsg == null || resultMsg.isEmpty()) {
      throw new IllegalArgumentException("Result message cannot be null");
    }
    if (sinkSchema == null || sinkSchema.isEmpty()) {
      throw new IllegalArgumentException("Sink schema cannot be null or empty");
    }

    this.translatedDdl = translatedDdl;
    this.ddlStatus = ddlStatus;
    this.resultMsg = resultMsg;
    this.sinkSchema = sinkSchema;
  }

  // Getters
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
