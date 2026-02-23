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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionRequest;
import org.apache.iceberg.connect.cmdb.dto.ddl.response.DdlExecutionResponse;
import org.apache.iceberg.connect.cmdb.dto.dml.request.DmlStatusPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.dml.response.DmlStatusResponse;
import org.apache.iceberg.connect.cmdb.model.DdlStatus;
import org.apache.iceberg.connect.cmdb.model.DmlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No-op implementation of CmdbManager for use when Kakao CDC is disabled. All methods return
 * default/empty values without making any API calls.
 */
public class NoOpCmdbManager extends CmdbManager {

  private static final Logger LOG = LoggerFactory.getLogger(NoOpCmdbManager.class);

  NoOpCmdbManager() {
    super();
  }

  @Override
  public long getLatestDdlVersion() {
    LOG.debug("[NoOpCmdbManager] getLatestDdlVersion called, returning UNDEFINED_DDL_VERSION");
    return UNDEFINED_DDL_VERSION;
  }

  @Override
  public List<DdlExecutionResponse> getDdlExecutions(Long ddlVersion, DdlStatus status) {
    LOG.debug("[NoOpCmdbManager] getDdlExecutions called, returning empty list");
    return Collections.emptyList();
  }

  @Override
  public List<DdlExecutionResponse> fetchDdlExecutions(Long ddlVersion, DdlStatus status) {
    LOG.debug("[NoOpCmdbManager] fetchDdlExecutions called, returning empty list");
    return Collections.emptyList();
  }

  @Override
  public DdlExecutionResponse checkDdlExecutionOrCreate(long ddlVersion, String ddl) {
    LOG.debug("[NoOpCmdbManager] checkDdlExecutionOrCreate called, returning null");
    return null;
  }

  @Override
  public DdlExecutionResponse postDdlExecution(DdlExecutionRequest request) {
    LOG.debug("[NoOpCmdbManager] postDdlExecution called, returning null");
    return null;
  }

  @Override
  public DdlExecutionResponse patchDdlExecutionResult(long id, DdlExecutionPatchRequest request) {
    LOG.debug("[NoOpCmdbManager] patchDdlExecutionResult called, returning null");
    return null;
  }

  @Override
  public void deleteDdlExecution(long id) {
    LOG.debug("[NoOpCmdbManager] deleteDdlExecution called, doing nothing");
  }

  @Override
  public Long findDmlConsumerStatusId(long partition, long ddlVersion) {
    LOG.debug("[NoOpCmdbManager] findDmlConsumerStatusId called, returning null");
    return null;
  }

  @Override
  public DmlStatusResponse getLatestDmlConsumerStatus(long partitionId) {
    LOG.debug("[NoOpCmdbManager] getLatestDmlConsumerStatus called, returning null");
    return null;
  }

  @Override
  public DmlStatusResponse getSingleDmlConsumerStatus(long partitionId, long ddlVersion) {
    LOG.debug("[NoOpCmdbManager] getSingleDmlConsumerStatus called, returning null");
    return null;
  }

  @Override
  public List<DmlStatusResponse> getDmlConsumerStatuses(long ddlVersion) {
    LOG.debug("[NoOpCmdbManager] getDmlConsumerStatuses called, returning empty list");
    return Collections.emptyList();
  }

  @Override
  public List<DmlStatusResponse> fetchDmlConsumerStatuses(long ddlVersion) {
    LOG.debug("[NoOpCmdbManager] fetchDmlConsumerStatuses called, returning empty list");
    return Collections.emptyList();
  }

  @Override
  public DmlStatusResponse postDmlConsumerStatus(
      long partitionId, long offset, long ddlVersion, DmlInfo dmlInfo) {
    LOG.debug("[NoOpCmdbManager] postDmlConsumerStatus called, returning null");
    return null;
  }

  @Override
  public void deleteDmlConsumerStatusById(long id) {
    LOG.debug("[NoOpCmdbManager] deleteDmlConsumerStatusById called, doing nothing");
  }

  @Override
  public DmlStatusResponse patchDmlConsumerStatus(long id, DmlStatusPatchRequest request) {
    LOG.debug("[NoOpCmdbManager] patchDmlConsumerStatus called, returning null");
    return null;
  }

  @Override
  public CompletableFuture<List<DmlStatusResponse>> getDmlConsumerStatusesAsync(
      Long partitionId, long ddlVersion) {
    LOG.debug("[NoOpCmdbManager] getDmlConsumerStatusesAsync called, returning empty list future");
    return CompletableFuture.completedFuture(Collections.emptyList());
  }

  @Override
  public void clearCaches() {
    LOG.debug("[NoOpCmdbManager] clearCaches called, doing nothing");
  }
}
