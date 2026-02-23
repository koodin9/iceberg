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
package org.apache.iceberg.connect.simpleserver;

import static org.apache.iceberg.connect.simpleserver.Utils.getIdFromPath;
import static org.apache.iceberg.connect.simpleserver.Utils.readJsonBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.cmdb.dto.dml.request.DmlStatusPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.dml.request.DmlStatusRequest;
import org.apache.iceberg.connect.cmdb.dto.dml.response.DmlStatusResponse;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Data API that Kafka Connect can call to get/post data
public class DmlStatusHandler extends HttpServlet {
  private Logger logger = LoggerFactory.getLogger(DmlStatusHandler.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private final Map<Long, DmlStatusResponse> dmlStatuses = Maps.newHashMap();

  /** Reset the handler state - clear all DML statuses. */
  public void reset() {
    dmlStatuses.clear();
    logger.info("DmlStatusHandler reset - all statuses cleared");
  }

  /** Initialize with pre-defined DML statuses for testing. */
  public void initializeWith(DmlStatusResponse... initialData) {
    dmlStatuses.clear();
    for (DmlStatusResponse status : initialData) {
      dmlStatuses.put(status.getId(), status);
    }
    logger.info("DmlStatusHandler initialized with {} entries", initialData.length);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    // Query Parameter 파싱
    String partitionParam = req.getParameter("partition_id");
    Integer partitionId = partitionParam != null ? Integer.parseInt(partitionParam) : null;
    String ddlVersionParam = req.getParameter("ddl_version");
    Long ddlVersion = ddlVersionParam != null ? Long.parseLong(ddlVersionParam) : null;
    logger.info(
        "Query params - partition_id: {}, ddl_version: {}", partitionParam, ddlVersionParam);

    Map<Long, DmlStatusResponse> filteredStatuses = filterDmlStatuses(partitionId, ddlVersion);
    logger.info("🟢🟢🟢 [DML Status] GET request received, returning {}", filteredStatuses.size());

    try {
      resp.setContentType("application/json");
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write(mapper.writeValueAsString(filteredStatuses.values()));
    } catch (Exception e) {
      logger.error("Error processing GET request", e);
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("{\"error\": \"Invalid request parameters\"}");
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

    // JSON Body 파싱
    String jsonBody = readJsonBody(req);
    logger.info("🟢🟢🟢 [DML Status] POST request received: {}", jsonBody);

    try {
      DmlStatusRequest dmlRequest = mapper.readValue(jsonBody, DmlStatusRequest.class);

      // 삭제를 전혀 하지 않기 때문에 size를 ID로 사용가능
      DmlStatusResponse newEntry =
          new DmlStatusResponse(
              dmlStatuses.size(),
              dmlRequest.getClusterName(),
              dmlRequest.getTableName(),
              dmlRequest.getPartitionId(),
              dmlRequest.getOffset(),
              dmlRequest.getDdlVersion(),
              dmlRequest.getLastProcessedDmlId(),
              dmlRequest.getStatus(), // 초기 상태
              "",
              "");
      dmlStatuses.put((long) dmlStatuses.size(), newEntry);

      resp.setContentType("application/json");
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write(mapper.writeValueAsString(newEntry));
    } catch (Exception e) {
      logger.error("Error processing POST request", e);
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("{\"error\": \"Invalid JSON format or request data\"}");
    }
  }

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    // PATCH 메서드 직접 처리
    if ("PATCH".equalsIgnoreCase(req.getMethod())) {
      handlePatch(req, resp);
    } else {
      try {
        super.service(req, resp);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  private void handlePatch(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String jsonBody = readJsonBody(req);
    Long id = getIdFromPath(req);
    if (id == null) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("{\"error\": \"ID is required in path\"}");
      return;
    }
    logger.info(
        "🟢🟢🟢 [DML Status] PATCH request received - ID: {}, Request body: {}", id, jsonBody);

    try {
      DmlStatusPatchRequest patchRequest = mapper.readValue(jsonBody, DmlStatusPatchRequest.class);

      // 기존값 수정
      replaceStatus(id, patchRequest);
      logger.info("🟢🟢🟢 [DML Status] PATCH result: {}", dmlStatuses.get(id));

      resp.setContentType("application/json");
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write(mapper.writeValueAsString(dmlStatuses.get(id)));
    } catch (NumberFormatException e) {
      logger.error("Invalid ID format", e);
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("{\"error\": \"Invalid ID format\"}");
    } catch (Exception e) {
      logger.error("Error processing PATCH request", e);
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("{\"error\": \"Invalid request data\"}");
    }
  }

  private void replaceStatus(long id, DmlStatusPatchRequest patchRequest) {
    dmlStatuses.compute(
        id,
        (k, old) ->
            new DmlStatusResponse(
                id,
                old.getClusterName(),
                old.getTableName(),
                old.getPartitionId(),
                patchRequest.getOffset() == null ? old.getOffset() : patchRequest.getOffset(),
                old.getDdlVersion(),
                patchRequest.getLastProcessedDmlId() == null
                    ? old.getLastProcessedDmlId()
                    : patchRequest.getLastProcessedDmlId(),
                patchRequest.getStatus() == null ? old.getStatus() : patchRequest.getStatus(),
                "",
                ""));
  }

  private Map<Long, DmlStatusResponse> filterDmlStatuses(Integer partitionId, Long ddlVersion) {
    return dmlStatuses.entrySet().stream()
        .filter(
            entry -> {
              DmlStatusResponse execution = entry.getValue();

              // partition 조건
              boolean partitionCondition = true;
              if (partitionId != null) {
                partitionCondition = execution.getPartitionId() == partitionId;
              }

              // version 조건
              boolean versionCondition = true;
              if (ddlVersion != null) {
                versionCondition = execution.getDdlVersion() == ddlVersion;
              }

              return partitionCondition && versionCondition;
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
