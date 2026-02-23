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
package org.apache.iceberg.connect;

import io.debezium.util.Stopwatch;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.auth.KerberosAuthManager;
import org.apache.iceberg.connect.cmdb.CmdbManagerFactory;
import org.apache.iceberg.connect.common.Alert;
import org.apache.iceberg.connect.common.AlertTarget;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);

  private Alert alert;
  private KerberosAuthManager authManager;
  private IcebergSinkConfig config;
  private Catalog catalog;
  private Committer committer;

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> props) {
    LOG.info("Starting Iceberg Sink Task");
    this.config = new IcebergSinkConfig(props);
    this.alert = new Alert(config);
    if (config.kerberosAuthentication()) {
      this.authManager = KerberosAuthManager.getInstance(config);
    }
    this.catalog = CatalogUtils.loadCatalog(config);

    /**
     * coordinator의 컨트롤 토픽 컨슈머는 auto.offset.reset 이 latest 로 설정됨 커넥터 실행 초기에 worker가 빠르게 생성한 이벤트를
     * 수신하지 못하는 경우가 있었음 따라서 coordinator의 컨트롤 토픽 컨슈머 초기화까지 여유시간을 둔다.
     */
    try {
      LOG.info("Waiting 10 seconds for the committer to initialize...");
      TimeUnit.SECONDS.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while waiting for the committer to initialize, proceeding anyway.", e);
    }

    this.committer = CommitterFactory.createCommitter(config);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    LOG.info("Opening Iceberg Sink Task");

    try {
      if (authManager != null) {
        authManager.executeWithAuth(
            () -> {
              System.setProperty("sun.security.krb5.debug", "true");
              committer.open(catalog, config, context, partitions);
              return null;
            });
      } else {
        committer.open(catalog, config, context, partitions);
      }
    } catch (Exception e) {
      String errorMessage = "Error opening sinkTask: " + e.getMessage();
      LOG.error("Error opening sinkTask", e);
      alert.sendNotificationMessage(errorMessage, AlertTarget.MANAGER, Level.ERROR);
      throw new ConnectException(errorMessage, e);
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    LOG.info("Closing Iceberg Sink Task");
    CmdbManagerFactory.clearCaches(this.config);
    committer.close(partitions);
  }

  private void close() {
    if (committer != null) {
      committer.close(List.of());
      committer = null;
    }

    if (catalog != null) {
      if (catalog instanceof AutoCloseable) {
        try {
          ((AutoCloseable) catalog).close();
        } catch (Exception e) {
          LOG.warn("An error occurred closing catalog instance, ignoring...", e);
        }
      }
      catalog = null;
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    try {
      if (committer != null) {
        if (authManager != null) {
          authManager.executeWithAuth(
              () -> {
                Stopwatch stopwatch = Stopwatch.reusable().start();
                committer.save(sinkRecords);
                stopwatch.stop();
                LOG.trace(
                    "[DDL Connector] [PERF] Task put {} records, {} ms",
                    sinkRecords.size(),
                    stopwatch.durations());
                return null;
              });
        } else {
          committer.save(sinkRecords);
        }
      }
    } catch (RebalanceInProgressException e) {
      // RebalanceInProgressException은 일시적인 에러이므로 RetriableException으로 변환
      // 다음 put() 호출까지 딜레이를 줘서 불필요한 빠른 재시도 방지
      context.timeout(5000L);
      throw new RetriableException("Rebalance in progress, retrying...", e);
    } catch (Exception e) {
      String errorMessage = "레코드 처리 중 에러 발생: " + e.getMessage();
      alert.sendNotificationMessage(errorMessage, AlertTarget.ALL, Level.ERROR);
      throw new ConnectException(errorMessage, e);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    if (committer != null) {
      committer.save(null);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // offset commit is handled by the worker
    return ImmutableMap.of();
  }

  @Override
  public void stop() {
    LOG.info("Stopping Iceberg Sink Task");
    close();
  }
}
