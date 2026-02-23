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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionRequest;
import org.apache.iceberg.connect.cmdb.dto.ddl.response.DdlExecutionResponse;
import org.apache.iceberg.connect.cmdb.dto.dml.request.DmlStatusPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.dml.response.DmlStatusResponse;
import org.apache.iceberg.connect.cmdb.exceptions.CmdbApiException;
import org.apache.iceberg.connect.cmdb.model.DdlStatus;
import org.apache.iceberg.connect.cmdb.model.DmlInfo;
import org.apache.iceberg.connect.cmdb.model.PartitionStatus;
import org.apache.iceberg.connect.http.HttpClientService;
import org.apache.iceberg.connect.http.HttpClientServiceFactory;
import org.apache.iceberg.connect.http.HttpException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;

class CmdbManagerTest {

  @Mock private IcebergSinkConfig config;

  @Mock private HttpClientService httpClientService;

  @Mock private HttpResponse<String> httpResponse;

  private CmdbManager cmdbManager;
  private ObjectMapper objectMapper;

  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_TABLE_NAME = "test.table";
  private static final String TEST_TOPIC_NAME = "test-topic";
  private static final String TEST_API_URL = "http://test-api.com";
  private static final String TEST_API_TOKEN = "test-token";

  @BeforeEach
  void before() {
    // Initialize mocks
    config = mock(IcebergSinkConfig.class);
    httpClientService = mock(HttpClientService.class);
    httpResponse = mock(HttpResponse.class);

    objectMapper = new ObjectMapper();

    // Mock config setup
    when(config.cmdbApiUrl()).thenReturn(TEST_API_URL);
    when(config.cmdbApiToken()).thenReturn(TEST_API_TOKEN);
    when(config.hadoopClusterName()).thenReturn(TEST_CLUSTER_NAME);
    when(config.tables()).thenReturn(List.of(TEST_TABLE_NAME));
    when(config.topics()).thenReturn(TEST_TOPIC_NAME);

    // Mock HttpClientServiceFactory
    try (MockedStatic<HttpClientServiceFactory> factory =
        mockStatic(HttpClientServiceFactory.class)) {
      factory
          .when(() -> HttpClientServiceFactory.getInstance(TEST_API_URL, TEST_API_TOKEN))
          .thenReturn(httpClientService);

      cmdbManager = new CmdbManager(config);
    }
  }

  /** DDL 수행 내역 중 최신 버전 획득 테스트 */
  @Test
  void testGetLatestDdlVersion_WithProcessedExecutions() throws Exception {
    // Given
    List<DdlExecutionResponse> ddlExecutions =
        Arrays.asList(
            createDdlExecutionResponse(1L, 10L, DdlStatus.PROCESSED),
            createDdlExecutionResponse(2L, 15L, DdlStatus.PROCESSED),
            createDdlExecutionResponse(3L, 5L, DdlStatus.PROCESSED));

    String responseJson = objectMapper.writeValueAsString(ddlExecutions);
    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When
    long latestVersion = cmdbManager.getLatestDdlVersion();

    // Then
    assertEquals(15L, latestVersion);
    verify(httpClientService).sendGet(contains("/api/v2/cdc/icebergsinkddlexecutions"));
  }

  /** DDL 수행내역이 없을 경우 기본값 반환 테스트 */
  @Test
  void testGetLatestDdlVersion_WithNoExecutions() throws Exception {
    // Given
    List<DdlExecutionResponse> emptyList = Collections.emptyList();
    String responseJson = objectMapper.writeValueAsString(emptyList);
    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When
    long latestVersion = cmdbManager.getLatestDdlVersion();

    // Then
    assertEquals(CmdbManager.UNDEFINED_DDL_VERSION, latestVersion);
  }

  /** */
  @Test
  void testFetchDdlExecutions_WithFilters() throws Exception {
    // Given
    List<DdlExecutionResponse> ddlExecutions =
        Arrays.asList(
            createDdlExecutionResponse(1L, 10L, DdlStatus.PROCESSED),
            createDdlExecutionResponse(2L, 11L, DdlStatus.RECEIVED));

    String responseJson = objectMapper.writeValueAsString(ddlExecutions);
    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When
    List<DdlExecutionResponse> result = cmdbManager.fetchDdlExecutions(10L, DdlStatus.PROCESSED);

    // Then
    assertEquals(2, result.size());
    verify(httpClientService).sendGet(contains("ddl_version=10"));
    verify(httpClientService).sendGet(contains("status=processed"));
  }

  /** DDL 수행 내역이 있을 경우 단순 조회 */
  @Test
  void testCheckDdlExecutionOrCreate_ExistingExecution() throws Exception {
    // Given
    long ddlVersion = 10L;

    List<DdlExecutionResponse> existingExecutions =
        List.of(createDdlExecutionResponse(1L, ddlVersion, DdlStatus.RECEIVED));

    String responseJson = objectMapper.writeValueAsString(existingExecutions);
    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When
    DdlExecutionResponse result =
        cmdbManager.checkDdlExecutionOrCreate(ddlVersion, "ALTER TABLE test");

    // Then
    assertNotNull(result);
    assertEquals(ddlVersion, result.getDdlVersion());
    verify(httpClientService, times(1)).sendGet(anyString());
    verify(httpClientService, never()).sendPost(anyString(), anyString());
  }

  /** DDL 수행 내역이 없을 경우 새로 생성 */
  @Test
  void testCheckDdlExecutionOrCreate_CreateNew() throws Exception {
    // Given
    long ddlVersion = 10L;

    // 첫 조회시 반환될 빈 목록
    List<DdlExecutionResponse> emptyList = Collections.emptyList();
    String emptyResponseJson = objectMapper.writeValueAsString(emptyList);

    // post 결과로 반환될 신규생성 항목
    DdlExecutionResponse newExecution =
        createDdlExecutionResponse(1L, ddlVersion, DdlStatus.RECEIVED);
    String newExecutionJson = objectMapper.writeValueAsString(newExecution);

    when(httpResponse.body()).thenReturn(emptyResponseJson).thenReturn(newExecutionJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);
    when(httpClientService.sendPost(anyString(), anyString())).thenReturn(httpResponse);

    // When
    DdlExecutionResponse result =
        cmdbManager.checkDdlExecutionOrCreate(ddlVersion, "CREATE TABLE test");

    // Then
    assertNotNull(result);
    assertEquals(ddlVersion, result.getDdlVersion());
    verify(httpClientService, times(1)).sendGet(anyString());
    verify(httpClientService, times(1)).sendPost(anyString(), anyString());
  }

  @Test
  void testPostDdlExecution() throws Exception {
    // Given
    DdlExecutionRequest request =
        new DdlExecutionRequest(TEST_CLUSTER_NAME, TEST_TABLE_NAME, 10L, "CREATE TABLE test");

    DdlExecutionResponse response = createDdlExecutionResponse(1L, 10L, DdlStatus.RECEIVED);
    String responseJson = objectMapper.writeValueAsString(response);

    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendPost(anyString(), anyString())).thenReturn(httpResponse);

    // When
    DdlExecutionResponse result = cmdbManager.postDdlExecution(request);

    // Then
    assertNotNull(result);
    assertEquals(10L, result.getDdlVersion());
    verify(httpClientService).sendPost(eq("/api/v2/cdc/icebergsinkddlexecutions"), anyString());
  }

  /** 3-arg 생성자를 사용한 DdlExecutionRequest 테스트 (초기 상태 기록용, status = PROCESSED) */
  @Test
  void testPostDdlExecution_WithInitialMarker() throws Exception {
    // Given - 3-arg 생성자 사용 (originalDdl 없음, status = PROCESSED)
    DdlExecutionRequest request = new DdlExecutionRequest(TEST_CLUSTER_NAME, TEST_TABLE_NAME, 10L);

    DdlExecutionResponse response = createDdlExecutionResponse(1L, 10L, DdlStatus.PROCESSED);
    String responseJson = objectMapper.writeValueAsString(response);

    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendPost(anyString(), anyString())).thenReturn(httpResponse);

    // When
    DdlExecutionResponse result = cmdbManager.postDdlExecution(request);

    // Then
    assertNotNull(result);
    assertEquals(10L, result.getDdlVersion());
    assertEquals(DdlStatus.PROCESSED, result.getStatus());
    verify(httpClientService, times(1)).sendPost(anyString(), anyString());
    verify(httpClientService).sendPost(eq("/api/v2/cdc/icebergsinkddlexecutions"), anyString());
  }

  @Test
  void testPatchDdlExecutionResult() throws Exception {
    // Given
    long id = 1L;
    DdlExecutionPatchRequest request =
        new DdlExecutionPatchRequest("TRANSLATED DDL", DdlStatus.PROCESSED, "Success", "schema");

    DdlExecutionResponse response = createDdlExecutionResponse(id, 10L, DdlStatus.PROCESSED);
    String responseJson = objectMapper.writeValueAsString(response);

    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendPatch(anyString(), anyString())).thenReturn(httpResponse);

    // When
    DdlExecutionResponse result = cmdbManager.patchDdlExecutionResult(id, request);

    // Then
    assertNotNull(result);
    assertEquals(id, result.getId());
    verify(httpClientService)
        .sendPatch(eq("/api/v2/cdc/icebergsinkddlexecutions/" + id), anyString());
  }

  @Test
  void testDeleteDdlExecution() {
    // Given
    long id = 1L;
    when(httpClientService.sendDelete(anyString())).thenReturn(httpResponse);

    // When
    cmdbManager.deleteDdlExecution(id);

    // Then
    verify(httpClientService).sendDelete(eq("/api/v2/cdc/icebergsinkddlexecutions/" + id));
  }

  @Test
  void testGetSingleDmlConsumerStatus_SingleResult() throws Exception {
    // Given
    long partitionId = 0L;
    long ddlVersion = 10L;
    long offset = 100L;

    List<DmlStatusResponse> dmlStatuses =
        Arrays.asList(
            createDmlStatusResponse(1L, partitionId, offset, ddlVersion),
            createDmlStatusResponse(2L, 1L, 200L, ddlVersion) // 다른 파티션
            );

    String responseJson = objectMapper.writeValueAsString(dmlStatuses);
    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When
    DmlStatusResponse result = cmdbManager.getSingleDmlConsumerStatus(partitionId, ddlVersion);

    // Then
    assertNotNull(result);
    assertEquals(partitionId, result.getPartitionId());
    assertEquals(offset, result.getOffset());
  }

  /** DML 소비자 상태 조회 시 여러 결과가 있을 경우, 가장 높은 offset을 가진 항목을 반환 */
  @Test
  void testGetSingleDmlConsumerStatus_MultipleResults() throws Exception {
    // Given
    long partitionId = 0L;
    long ddlVersion = 10L;

    List<DmlStatusResponse> dmlStatuses =
        Arrays.asList(
            createDmlStatusResponse(1L, partitionId, 100L, ddlVersion),
            createDmlStatusResponse(2L, partitionId, 200L, ddlVersion));

    String responseJson = objectMapper.writeValueAsString(dmlStatuses);
    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When
    DmlStatusResponse result = cmdbManager.getSingleDmlConsumerStatus(partitionId, ddlVersion);

    // Then
    assertNotNull(result);
    assertEquals(partitionId, result.getPartitionId());
    assertEquals(200L, result.getOffset());
  }

  @Test
  void testGetSingleDmlConsumerStatus_NoResults() throws Exception {
    // Given
    long partitionId = 0L;
    long ddlVersion = 10L;

    List<DmlStatusResponse> emptyList = Collections.emptyList();
    String responseJson = objectMapper.writeValueAsString(emptyList);
    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When
    DmlStatusResponse result = cmdbManager.getSingleDmlConsumerStatus(partitionId, ddlVersion);

    // Then
    assertNull(result);
  }

  @Test
  void testPostDmlConsumerStatus() throws Exception {
    // Given
    long partitionId = 0L;
    long offset = 100L;
    long ddlVersion = 10L;
    DmlInfo dmlInfo = new DmlInfo(Map.of("gtid", "test-gtid", "pos", "123"));

    DmlStatusResponse response = createDmlStatusResponse(1L, partitionId, offset, ddlVersion);
    String responseJson = objectMapper.writeValueAsString(response);

    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendPost(anyString(), anyString())).thenReturn(httpResponse);

    // When
    DmlStatusResponse result =
        cmdbManager.postDmlConsumerStatus(partitionId, offset, ddlVersion, dmlInfo);

    // Then
    assertNotNull(result);
    assertEquals(partitionId, result.getPartitionId());
    assertEquals(offset, result.getOffset());
    verify(httpClientService)
        .sendPost(eq("/api/v2/cdc/icebergsinkdmlconsumerstatuses"), anyString());
  }

  @Test
  void testPatchDmlConsumerStatus() throws Exception {
    // Given
    long id = 1L;
    DmlStatusPatchRequest request =
        new DmlStatusPatchRequest(
            200L,
            "{\"pos\": \"699\", \"row\": \"0\", \"gtid\": \"570555db-508b-11f0-926d-fa163edc76a5:2355\"}",
            PartitionStatus.DONE);

    DmlStatusResponse response = createDmlStatusResponse(id, 0L, 200L, 10L);
    String responseJson = objectMapper.writeValueAsString(response);

    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendPatch(anyString(), anyString())).thenReturn(httpResponse);

    // When
    DmlStatusResponse result = cmdbManager.patchDmlConsumerStatus(id, request);

    // Then
    assertNotNull(result);
    assertEquals(id, result.getId());
    verify(httpClientService)
        .sendPatch(eq("/api/v2/cdc/icebergsinkdmlconsumerstatuses/" + id), anyString());
  }

  @Test
  void testDeleteDmlConsumerStatusById() {
    // Given
    long id = 1L;
    when(httpClientService.sendDelete(anyString())).thenReturn(httpResponse);

    // When
    cmdbManager.deleteDmlConsumerStatusById(id);

    // Then
    verify(httpClientService).sendDelete(eq("/api/v2/cdc/icebergsinkdmlconsumerstatuses/" + id));
  }

  @Test
  void testGetDmlConsumerStatusesAsync() throws Exception {
    // Given
    Long partitionId = 0L;
    long ddlVersion = 10L;

    List<DmlStatusResponse> dmlStatuses =
        List.of(createDmlStatusResponse(1L, partitionId, 100L, ddlVersion));

    String responseJson = objectMapper.writeValueAsString(dmlStatuses);
    when(httpResponse.body()).thenReturn(responseJson);

    CompletableFuture<HttpResponse<String>> futureResponse =
        CompletableFuture.completedFuture(httpResponse);
    when(httpClientService.sendGetAsync(anyString())).thenReturn(futureResponse);

    // When
    CompletableFuture<List<DmlStatusResponse>> result =
        cmdbManager.getDmlConsumerStatusesAsync(partitionId, ddlVersion);

    // Then
    List<DmlStatusResponse> actualResult = result.get();
    assertNotNull(actualResult);
    assertEquals(1, actualResult.size());
    assertEquals(partitionId, actualResult.get(0).getPartitionId());
  }

  @Test
  void testHttpExceptionHandling() {
    // Given
    HttpException httpException = new HttpException(500, "API Error");
    when(httpClientService.sendGet(anyString())).thenThrow(new RuntimeException(httpException));

    // When & Then
    CmdbApiException exception =
        assertThrows(CmdbApiException.class, () -> cmdbManager.getLatestDdlVersion());

    assertTrue(exception.getMessage().contains("Failed to Get DDL executions"));
    assertEquals(httpException, exception.getCause());
  }

  @Test
  void testJsonProcessingExceptionHandling() {
    // Given
    when(httpResponse.body()).thenReturn("invalid json");
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When & Then
    CmdbApiException exception =
        assertThrows(CmdbApiException.class, () -> cmdbManager.getLatestDdlVersion());

    assertTrue(exception.getMessage().contains("Failed to process JSON"));
  }

  @Test
  void testFindDmlConsumerStatusId_Found() throws Exception {
    // Given
    long partitionId = 0L;
    long ddlVersion = 10L;
    long expectedId = 123L;

    List<DmlStatusResponse> dmlStatuses =
        List.of(createDmlStatusResponse(expectedId, partitionId, 100L, ddlVersion));

    String responseJson = objectMapper.writeValueAsString(dmlStatuses);
    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When
    Long result = cmdbManager.findDmlConsumerStatusId(partitionId, ddlVersion);

    // Then
    assertEquals(expectedId, result);
  }

  @Test
  void testFindDmlConsumerStatusId_NotFound() throws Exception {
    // Given
    long partitionId = 0L;
    long ddlVersion = 10L;

    List<DmlStatusResponse> emptyList = Collections.emptyList();
    String responseJson = objectMapper.writeValueAsString(emptyList);
    when(httpResponse.body()).thenReturn(responseJson);
    when(httpClientService.sendGet(anyString())).thenReturn(httpResponse);

    // When
    Long result = cmdbManager.findDmlConsumerStatusId(partitionId, ddlVersion);

    // Then
    assertNull(result);
  }

  private DdlExecutionResponse createDdlExecutionResponse(
      Long id, Long ddlVersion, DdlStatus status) {
    return new DdlExecutionResponse(
        id,
        TEST_CLUSTER_NAME,
        TEST_TABLE_NAME,
        ddlVersion,
        "CREATE TABLE test",
        "TRANSLATED DDL",
        status,
        "Success",
        "schema",
        "2023-01-01T00:00:00Z",
        "2023-01-01T00:00:00Z");
  }

  private DmlStatusResponse createDmlStatusResponse(
      Long id, Long partitionId, Long offset, Long ddlVersion) {
    return new DmlStatusResponse(
        id,
        TEST_CLUSTER_NAME,
        TEST_TABLE_NAME,
        partitionId,
        offset,
        ddlVersion,
        "{\"pos\": \"16023\", \"row\": \"0\", \"gtid\": \"fbb4d0b4-fb35-11ef-a81d-fa163e17d8ff:78\"}",
        PartitionStatus.IN_PROGRESS,
        "2023-01-01T00:00:00Z",
        "2023-01-01T00:00:00Z");
  }
}
