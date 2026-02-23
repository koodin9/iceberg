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
package org.apache.iceberg.connect.cmdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionRequest;
import org.apache.iceberg.connect.cmdb.dto.ddl.response.DdlExecutionResponse;
import org.apache.iceberg.connect.cmdb.dto.dml.request.DmlStatusPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.dml.request.DmlStatusRequest;
import org.apache.iceberg.connect.cmdb.dto.dml.response.DmlStatusResponse;
import org.apache.iceberg.connect.cmdb.exceptions.CmdbApiException;
import org.apache.iceberg.connect.cmdb.model.DdlStatus;
import org.apache.iceberg.connect.cmdb.model.DmlInfo;
import org.apache.iceberg.connect.cmdb.model.PartitionStatus;
import org.apache.iceberg.connect.common.CmdbResponseCache;
import org.apache.iceberg.connect.http.HttpClientService;
import org.apache.iceberg.connect.http.HttpClientServiceFactory;
import org.apache.iceberg.connect.http.HttpException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmdbManager {

  private static final Logger LOG = LoggerFactory.getLogger(CmdbManager.class);

  private static final String API_PATH_PREFIX = "/api/v2/cdc";
  private static final String DDL_EXECUTION_ENDPOINT =
      API_PATH_PREFIX + "/icebergsinkddlexecutions";
  private static final String DML_CONSUMER_STATUS_ENDPOINT =
      API_PATH_PREFIX + "/icebergsinkdmlconsumerstatuses";

  public static final long UNDEFINED_DDL_VERSION = -1;
  public static final int DDL_VERSION_CACHE_SIZE = 3;

  private final HttpClientService httpClientService;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final String clusterName;
  private final String tableName;
  private final String topicName;
  private final String logHeader;
  private final CmdbResponseCache<DdlExecutionResponse> ddlExecutionCache;
  private final CmdbResponseCache<List<DmlStatusResponse>> dmlStatusCache;

  /** Protected no-arg constructor for NoOpCmdbManager subclass */
  protected CmdbManager() {
    this.httpClientService = null;
    this.clusterName = null;
    this.tableName = null;
    this.topicName = null;
    this.logHeader = "[NoOpCmdbManager]";
    this.ddlExecutionCache = null;
    this.dmlStatusCache = null;
  }

  CmdbManager(IcebergSinkConfig config) {
    this.httpClientService =
        HttpClientServiceFactory.getInstance(config.cmdbApiUrl(), config.cmdbApiToken());

    this.clusterName = config.hadoopClusterName();
    this.tableName = config.tables().get(0); // tables() null, empty 여부는 factory 에서 체크됨
    this.topicName = config.topics();
    this.logHeader = String.format("[CmdbManager for %s - %s]", clusterName, tableName);
    this.ddlExecutionCache = new CmdbResponseCache<>(DDL_VERSION_CACHE_SIZE);
    this.dmlStatusCache = new CmdbResponseCache<>(DDL_VERSION_CACHE_SIZE);

    LOG.info("🔧 {} CmdbManager initialized", logHeader);
    LOG.info("🔧 {} cmdbApiUrl: {}", logHeader, config.cmdbApiUrl());
    LOG.info("🔧 {} cmdbApiToken: {}", logHeader, config.cmdbApiToken());
  }

  /**
   * 현재 DDL 버전을 조회한다. (캐시 우선)
   *
   * @return PROCESSED 상태의 최신 DDL 버전. 실행 기록이 없을 경우 UNDEFINED_DDL_VERSION (-1)
   */
  public long getLatestDdlVersion() throws CmdbApiException {
    List<DdlExecutionResponse> ddlExecutions = getDdlExecutions(null, DdlStatus.PROCESSED);

    long result =
        ddlExecutions.stream()
            .map(DdlExecutionResponse::getDdlVersion)
            .max(Long::compareTo)
            .orElse(UNDEFINED_DDL_VERSION);

    LOG.info("{} [getLatestDdlVersion] Return Latest DDL Version: {}", logHeader, result);
    return result;
  }

  /**
   * DDL 실행 기록 캐시를 조회한다. 조건에 맞는 항목이 없을 경우 CMDB API 를 호출한다.
   *
   * @param ddlVersion DDL 버전 (null 이면 모든 버전 조회)
   * @param status DDL 상태 (null 이면 모든 상태 조회)
   * @return DDL 실행 기록 리스트
   */
  public List<DdlExecutionResponse> getDdlExecutions(Long ddlVersion, DdlStatus status)
      throws CmdbApiException {

    // 캐시에서 먼저 확인
    List<DdlExecutionResponse> ddlExecutions =
        ddlExecutionCache.getAll().stream()
            .filter(execution -> ddlVersion == null || execution.getDdlVersion() == ddlVersion)
            .filter(execution -> status == null || execution.getStatus() == status)
            .toList();

    if (!ddlExecutions.isEmpty()) {
      return ddlExecutions;
    }

    LOG.info("{} [getDdlExecutions] No cache for {}, {}", logHeader, ddlVersion, status);
    return fetchDdlExecutions(ddlVersion, status);
  }

  /**
   * CMDB API 를 호출하여 DDL 실행 기록을 조회하고 캐시에 기록한다
   *
   * @param ddlVersion DDL 버전 (null 이면 모든 버전 조회)
   * @param status DDL 상태 (null 이면 모든 상태 조회)
   * @return DDL 실행 기록 리스트
   */
  public List<DdlExecutionResponse> fetchDdlExecutions(Long ddlVersion, DdlStatus status)
      throws CmdbApiException {

    Map<String, String> params = getDefaultParamMap(false);
    if (ddlVersion != null) {
      params.put("ddl_version", String.valueOf(ddlVersion));
    }
    if (status != null) {
      params.put("status", status.getValue());
    }

    String url = buildUrlWithParams(DDL_EXECUTION_ENDPOINT, params);

    return executeWithExceptionHandling(
        "Get DDL executions: " + params,
        () -> {
          HttpResponse<String> response = httpClientService.sendGet(url);

          // 검색 결과 중 ddl version 값이 큰 n개(= 캐시사이즈) 를 캐시에 기록한다.
          List<DdlExecutionResponse> result =
              objectMapper.readValue(response.body(), new TypeReference<>() {});

          List<DdlExecutionResponse> targets =
              result.stream()
                  .sorted(Comparator.comparingLong(DdlExecutionResponse::getDdlVersion).reversed())
                  .limit(DDL_VERSION_CACHE_SIZE)
                  .toList();

          LOG.info(
              "{} Insert {} executions to cache: {}",
              logHeader,
              targets.size(),
              targets.stream().map(DdlExecutionResponse::getDdlVersion).toList());

          targets.forEach(r -> ddlExecutionCache.put(r.getDdlVersion(), r));
          return targets;
        });
  }

  /**
   * 특정 버전의 DDL 실행기록을 조회한다. 없을경우 신규 생성한다.
   *
   * @return 조회 또는 생성된 DDL 실행기록
   */
  public DdlExecutionResponse checkDdlExecutionOrCreate(long ddlVersion, String ddl)
      throws CmdbApiException {

    DdlExecutionRequest request = new DdlExecutionRequest(clusterName, tableName, ddlVersion, ddl);

    return executeWithExceptionHandling(
        "Get DDL execution, or Post if not exists: " + request,
        () -> {
          List<DdlExecutionResponse> existingExecutions = getDdlExecutions(ddlVersion, null);

          DdlExecutionResponse result;
          if (existingExecutions.isEmpty()) {
            result = postDdlExecution(request);
          } else {
            result = existingExecutions.get(0);
          }

          // 결과를 캐시에 저장
          ddlExecutionCache.put(result.getDdlVersion(), result);
          return result;
        });
  }

  /**
   * DDL 실행 기록을 생성한다.
   *
   * @return 생성된 DDL 실행 기록
   */
  public DdlExecutionResponse postDdlExecution(DdlExecutionRequest request)
      throws CmdbApiException {

    LOG.info("🔔 {} [POST DDL] Starting DDL execution creation request", logHeader);
    LOG.info("🔔 {} [POST DDL] Request details: {}", logHeader, request);

    return executeWithExceptionHandling(
        "Post DDL execution: " + request,
        () -> {
          String jsonPayload = objectMapper.writeValueAsString(request);
          HttpResponse<String> response =
              httpClientService.sendPost(DDL_EXECUTION_ENDPOINT, jsonPayload);
          DdlExecutionResponse result =
              objectMapper.readValue(response.body(), DdlExecutionResponse.class);

          // 새로 생성된 결과를 캐시에 저장
          ddlExecutionCache.put(result.getDdlVersion(), result);
          return result;
        });
  }

  /**
   * DDL 실행 기록을 업데이트한다.
   *
   * @return 업데이트된 DDL 실행 기록
   */
  public DdlExecutionResponse patchDdlExecutionResult(long id, DdlExecutionPatchRequest request)
      throws CmdbApiException {

    String url = buildUrlWithId(DDL_EXECUTION_ENDPOINT, id);
    LOG.info("🚧 {} [PATCH DDL] Starting DDL execution update request", logHeader);
    LOG.info("🚧 {} [PATCH DDL] Update details: id={}, {}", logHeader, id, request);

    return executeWithExceptionHandling(
        "Patch DDL execution record: id " + id + ", " + request,
        () -> {
          String jsonPayload = objectMapper.writeValueAsString(request);
          HttpResponse<String> response = httpClientService.sendPatch(url, jsonPayload);
          DdlExecutionResponse result =
              objectMapper.readValue(response.body(), DdlExecutionResponse.class);

          // 업데이트된 결과를 캐시에 반영
          ddlExecutionCache.put(result.getDdlVersion(), result);

          return result;
        });
  }

  public void deleteDdlExecution(long id) throws CmdbApiException {
    String url = buildUrlWithId(DDL_EXECUTION_ENDPOINT, id);

    executeWithExceptionHandling(
        "Delete DDL execution record: id " + id,
        () -> {
          HttpResponse<String> response = httpClientService.sendDelete(url);

          // 삭제된 경우 캐시에서도 제거 (ID로는 DDL 버전을 알 수 없으므로 캐시 전체 클리어)
          // 삭제를 쓸 일이 생긴다면 고도화 필요.
          ddlExecutionCache.clear();
          LOG.debug("{} Cleared DDL execution cache due to deletion of id: {}", logHeader, id);

          return response;
        });
  }

  /** DML consumer status 의 ID를 조회한다. 파티션 및 ddl 버전에 맞는 consumer status 가 없을 경우 null 을 반환한다. */
  public Long findDmlConsumerStatusId(long partition, long ddlVersion) {
    DmlStatusResponse response = getSingleDmlConsumerStatus(partition, ddlVersion);
    return (response != null) ? response.getId() : null;
  }

  /**
   * 지정한 파티션에 대해 ddl 버전과 관계 없이 최신 DML consumer 상태를 얻는다.
   *
   * <p>DDL 메세지 사이에 DML이 없거나 적어서 이전 DDL 버전의 DML 처리 기록이 없는 경우에도 DDL 메세지 헤더가 가리키는 last_dml_info 와 비교할
   * 처리현황을 얻기 위해 사용한다.
   */
  public DmlStatusResponse getLatestDmlConsumerStatus(long partitionId) {
    Map<String, String> params = getDefaultParamMap(true);
    params.put("partition_id", String.valueOf(partitionId));

    String url = buildUrlWithParams(DML_CONSUMER_STATUS_ENDPOINT, params);

    return executeWithExceptionHandling(
        "Get Dml Consumer Statuses with partition: " + partitionId,
        () -> {
          HttpResponse<String> response = httpClientService.sendGet(url);

          return objectMapper
              .readValue(response.body(), new TypeReference<List<DmlStatusResponse>>() {})
              .stream()
              .max(Comparator.comparingLong(DmlStatusResponse::getDdlVersion))
              .orElse(null);
        });
  }

  /**
   * cluster, table, topic, partition, ddl_version 컬럼 조합은 테이블 내에서 유일하다. 둘 이상의 status가 확인될 경우 경고 로그를
   * 남기고, offset이 가장 큰 status를 반환한다.
   *
   * @return 조회된 consumer status. 조회되지 않을 경우 null
   */
  public DmlStatusResponse getSingleDmlConsumerStatus(long partitionId, long ddlVersion) {
    List<DmlStatusResponse> statuses =
        new ArrayList<>(
            getDmlConsumerStatuses(ddlVersion).stream()
                .filter(t -> t.getPartitionId() == partitionId)
                .toList());

    if (statuses.isEmpty()) {
      LOG.info(
          "{} No DML consumer status: partition {}, ddl_version {}",
          logHeader,
          partitionId,
          ddlVersion);

      return null;
    }

    if (statuses.size() > 1) {
      LOG.warn("{} Multiple DML consumer statuses detected", logHeader);
      for (DmlStatusResponse status : statuses) {
        LOG.warn("{} DML consumer status: {}", logHeader, status);
      }

      statuses.sort(Comparator.comparingLong(DmlStatusResponse::getOffset).reversed());
    }

    return statuses.get(0);
  }

  /**
   * DML consumer 상태 캐시를 조회한다. 지정된 DDL 버전에 해당하는 값이 없을 경우 CMDB API 를 호출한다.
   *
   * @param ddlVersion DDL 버전
   * @return 지정된 ddl 버전의 파티션별 consumer 상태 목록
   */
  public List<DmlStatusResponse> getDmlConsumerStatuses(long ddlVersion) throws CmdbApiException {

    // 캐시에서 먼저 확인
    Optional<List<DmlStatusResponse>> dmlStatusList = dmlStatusCache.get(ddlVersion);

    return dmlStatusList.orElseGet(() -> fetchDmlConsumerStatuses(ddlVersion));
  }

  /**
   * CMDB API 에서 특정 DDL 버전에 해당하는 DML consumer 상태를 조회하고 캐시를 갱신한다.
   *
   * @param ddlVersion DDL 버전
   * @return 지정된 ddl 버전의 파티션별 consumer 상태 목록
   */
  public List<DmlStatusResponse> fetchDmlConsumerStatuses(long ddlVersion) throws CmdbApiException {

    Map<String, String> params = getDefaultParamMap(true);
    params.put("ddl_version", String.valueOf(ddlVersion));

    String url = buildUrlWithParams(DML_CONSUMER_STATUS_ENDPOINT, params);

    return executeWithExceptionHandling(
        "Get Dml Consumer Statuses with DDL Version: " + ddlVersion,
        () -> {
          HttpResponse<String> response = httpClientService.sendGet(url);
          List<DmlStatusResponse> statuses =
              objectMapper.readValue(response.body(), new TypeReference<>() {});

          dmlStatusCache.put(ddlVersion, statuses);
          return statuses;
        });
  }

  public DmlStatusResponse postDmlConsumerStatus(
      long partitionId, long offset, long ddlVersion, DmlInfo dmlInfo) throws CmdbApiException {
    DmlStatusRequest request =
        new DmlStatusRequest(
            clusterName,
            tableName,
            topicName,
            partitionId,
            offset,
            ddlVersion,
            dmlInfo.toJson(),
            PartitionStatus.IN_PROGRESS);

    LOG.info("🔔 {} [POST DML] Starting DML consumer status creation request", logHeader);
    LOG.info("🔔 {} [POST DML] Request details: {}", logHeader, request);

    return executeWithExceptionHandling(
        "Post DML consumer status: " + request,
        () -> {
          String jsonPayload = objectMapper.writeValueAsString(request);
          HttpResponse<String> response =
              httpClientService.sendPost(DML_CONSUMER_STATUS_ENDPOINT, jsonPayload);
          DmlStatusResponse result =
              objectMapper.readValue(response.body(), DmlStatusResponse.class);

          replaceDmlStatusCache(ddlVersion, result);
          return result;
        });
  }

  public void deleteDmlConsumerStatusById(long id) throws CmdbApiException {
    String url = buildUrlWithId(DML_CONSUMER_STATUS_ENDPOINT, id);

    executeWithExceptionHandling(
        "Delete DML consumer status: id " + id, () -> httpClientService.sendDelete(url));
  }

  /** DML consumer status의 상태를 업데이트 */
  public DmlStatusResponse patchDmlConsumerStatus(long id, DmlStatusPatchRequest request)
      throws CmdbApiException {
    // TODO: async 구성하여 Coordinator::updateConsumerStatusDDLVersion 에서 활용
    String url = buildUrlWithId(DML_CONSUMER_STATUS_ENDPOINT, id);

    LOG.info("🚧 {} [PATCH DML] Starting DML consumer status update request", logHeader);
    LOG.info("🚧 {} [PATCH DML] Update details: id={}, {}", logHeader, id, request);

    String operation = "Patch DML consumer status id " + id + ": " + request;
    return executeWithExceptionHandling(
        operation,
        () -> {
          String jsonPayload = objectMapper.writeValueAsString(request);
          HttpResponse<String> response = httpClientService.sendPatch(url, jsonPayload);
          DmlStatusResponse result =
              objectMapper.readValue(response.body(), DmlStatusResponse.class);

          replaceDmlStatusCache(result.getDdlVersion(), result);
          return result;
        });
  }

  /**
   * DmlStatus 캐시는 topic partition 별 컨슈머 상태를 ddl version 별로 저장하기 때문에 그 값이 List. 업데이트 (post or
   * patch)는 partition 별로 수행되므로, 캐시된 List 의 일부를 수정해야 한다.
   */
  private void replaceDmlStatusCache(long ddlVersion, DmlStatusResponse newItem) {
    // 캐시에서 기존 DML 상태를 가져오거나 빈 리스트 생성 (얕은복사)
    List<DmlStatusResponse> currentList =
        new ArrayList<>(dmlStatusCache.get(ddlVersion).orElseGet(Lists::newArrayList));

    // 기존 항목을 대체
    currentList.removeIf(t -> t.getPartitionId() == newItem.getPartitionId());
    currentList.add(newItem);

    // 새로 생성한 리스트로 기존 캐시값 대체
    dmlStatusCache.put(ddlVersion, currentList);
  }

  private String buildUrlWithParams(String endpoint, Map<String, String> params) {
    String queryParams =
        params.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining("&"));

    return endpoint + (queryParams.isEmpty() ? "" : "?" + queryParams);
  }

  private String buildUrlWithId(String endpoint, Long id) {
    return endpoint + (id == null ? "" : "/" + id);
  }

  @FunctionalInterface
  protected interface CheckedSupplier<T> {

    T get() throws Exception;
  }

  protected <T> T executeWithExceptionHandling(
      String operation, CheckedSupplier<T> operationSupplier) throws CmdbApiException {
    try {
      return operationSupplier.get();
    } catch (JsonProcessingException e) {
      LOG.error("{} Failed to process JSON while {}: {}", logHeader, operation, e.getMessage(), e);
      throw new CmdbApiException("Failed to process JSON while " + operation, e);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof HttpException) {
        LOG.error("{} Failed to {}: {}", logHeader, operation, e.getCause().getMessage(), e);
        throw new CmdbApiException("Failed to " + operation, e.getCause());
      }
      throw e; // 다른 RuntimeException은 그대로 전파
    } catch (Exception e) {
      LOG.error("{} Unexpected error while {}: {}", logHeader, operation, e.getMessage(), e);
      throw new CmdbApiException("Unexpected error while " + operation, e);
    }
  }

  // async methods

  /** 비동기로 DML consumer status들을 조회 */
  public CompletableFuture<List<DmlStatusResponse>> getDmlConsumerStatusesAsync(
      Long partitionId, long ddlVersion) {
    Map<String, String> params = getDefaultParamMap(true);
    if (partitionId != null) {
      params.put("partition_id", String.valueOf(partitionId));
    }
    params.put("ddl_version", String.valueOf(ddlVersion));

    String url = buildUrlWithParams(DML_CONSUMER_STATUS_ENDPOINT, params);
    String operation =
        "Get Dml Consumer Statuses async: partition " + partitionId + " ddlVersion " + ddlVersion;

    return httpClientService
        .sendGetAsync(url)
        .thenApply(
            response -> {
              try {
                return objectMapper.readValue(
                    response.body(), new TypeReference<List<DmlStatusResponse>>() {});
              } catch (JsonProcessingException e) {
                LOG.error(
                    "{} Failed to process JSON while {}: {}",
                    logHeader,
                    operation,
                    e.getMessage(),
                    e);
                throw new CmdbApiException("Failed to process JSON while " + operation, e);
              }
            })
        .exceptionally(
            throwable -> {
              LOG.error("{} Failed to {}: {}", logHeader, operation, throwable.getMessage());
              throw new CmdbApiException("Failed to " + operation, throwable);
            });
  }

  public void clearCaches() {
    ddlExecutionCache.clear();
    dmlStatusCache.clear();
    LOG.info("{} Cleared all caches", logHeader);
  }

  private Map<String, String> getDefaultParamMap(boolean useTopic) {
    Map<String, String> params = Maps.newHashMap();
    params.put("cluster_name", clusterName);
    params.put("table_name", tableName);
    if (useTopic) {
      params.put("topic_name", topicName);
    }

    return params;
  }
}
