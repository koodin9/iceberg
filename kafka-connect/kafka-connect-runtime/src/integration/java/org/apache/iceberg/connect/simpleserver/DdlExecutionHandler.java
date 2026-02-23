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
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionRequest;
import org.apache.iceberg.connect.cmdb.dto.ddl.response.DdlExecutionResponse;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 통합 테스트에서 kafka connector 의 CMDB API call 을 처리
public class DdlExecutionHandler extends HttpServlet {
  private final Logger logger = LoggerFactory.getLogger(DdlExecutionHandler.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private final Map<Long, DdlExecutionResponse> ddlExecutions = Maps.newHashMap();

  public DdlExecutionHandler() {
    logger.info("DdlExecutionHandler initialized with empty cache");
  }

  /** Reset the handler state - clear all DDL executions. */
  public void reset() {
    ddlExecutions.clear();
    logger.info("DdlExecutionHandler reset - all executions cleared");
  }

  /**
   * Initialize the handler with specific DDL executions for testing. This allows each test to set
   * up its own initial state.
   */
  public void initializeWith(DdlExecutionResponse... initialData) {
    ddlExecutions.clear();
    long id = 0;
    for (DdlExecutionResponse entry : initialData) {
      ddlExecutions.put(id++, entry);
    }
    logger.info("DdlExecutionHandler initialized with {} entries", initialData.length);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {

    // Query Parameter 파싱
    String statusParam = req.getParameter("status");
    String ddlVersionParam = req.getParameter("ddl_version");
    logger.info("🚀🚀🚀 [DDL Execution] GET request received, returning {}", ddlExecutions.size());

    Map<Long, DdlExecutionResponse> filteredExecutions =
        filterDdlExecutions(
            statusParam, ddlVersionParam != null ? Long.parseLong(ddlVersionParam) : null);

    try {
      resp.setContentType("application/json");
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write(mapper.writeValueAsString(filteredExecutions.values()));
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
    logger.info("🚀🚀🚀 [DDL Execution] POST request received: {}", jsonBody);

    try {
      DdlExecutionRequest ddlRequest = mapper.readValue(jsonBody, DdlExecutionRequest.class);

      // 삭제를 전혀 하지 않기 때문에 size를 ID로 사용가능
      DdlExecutionResponse newEntry =
          new DdlExecutionResponse(
              ddlExecutions.size(),
              ddlRequest.getClusterName(),
              ddlRequest.getTableName(),
              ddlRequest.getDdlVersion(),
              ddlRequest.getOriginalDdl(),
              ddlRequest.getTranslatedDdl(), // translatedDdl는 아직 없음
              ddlRequest.getStatus(), // 초기 상태
              ddlRequest.getResultMsg(), // resultMsg는 아직 없음
              ddlRequest.getSinkSchema(), // sinkSchema는 아직 없음
              "",
              "");
      ddlExecutions.put((long) ddlExecutions.size(), newEntry);

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
        "🚀🚀🚀 [DDL Execution] PATCH request received - ID: {}, Request body: {}", id, jsonBody);

    try {
      DdlExecutionPatchRequest patchRequest =
          mapper.readValue(jsonBody, DdlExecutionPatchRequest.class);

      // 기존값 수정
      replaceExecution(id, patchRequest);
      logger.info("🚀🚀🚀 [DDL Execution] PATCH result: {}", ddlExecutions.get(id));

      resp.setContentType("application/json");
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write(mapper.writeValueAsString(ddlExecutions.get(id)));

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

  private void replaceExecution(long id, DdlExecutionPatchRequest patchRequest) {
    logger.info("Updating DDL request with id: {}, status: {}", id, patchRequest.getStatus());

    ddlExecutions.compute(
        id,
        (k, old) ->
            new DdlExecutionResponse(
                id,
                old.getClusterName(),
                old.getTableName(),
                old.getDdlVersion(),
                old.getOriginalDdl(),
                patchRequest.getTranslatedDdl() == null
                    ? old.getTranslatedDdl()
                    : patchRequest.getTranslatedDdl(),
                patchRequest.getStatus() == null ? old.getStatus() : patchRequest.getStatus(),
                patchRequest.getResultMsg() == null
                    ? old.getResultMsg()
                    : patchRequest.getResultMsg(),
                patchRequest.getSinkSchema() == null
                    ? old.getSinkSchema()
                    : patchRequest.getSinkSchema(),
                "",
                ""));
  }

  private Map<Long, DdlExecutionResponse> filterDdlExecutions(String status, Long ddlVersion) {
    return ddlExecutions.entrySet().stream()
        .filter(
            entry -> {
              DdlExecutionResponse execution = entry.getValue();

              // status 조건
              boolean statusCondition = true;
              if (status != null) {
                statusCondition = execution.getStatus().getValue().equals(status);
              }

              // version 조건
              boolean versionCondition = true;
              if (ddlVersion != null) {
                versionCondition = execution.getDdlVersion() == ddlVersion;
              }

              return statusCondition && versionCondition;
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
