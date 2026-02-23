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

import java.util.Map;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CmdbManager 인스턴스를 커넥터별로 관리하는 팩토리 클래스
 *
 * <p>고유한 (clusterName, tableName) 조합에 대해 하나의 CmdbManager 인스턴스만 생성
 */
public class CmdbManagerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CmdbManagerFactory.class);

  private static final Map<String, CmdbManager> INSTANCES = Maps.newConcurrentMap();

  private CmdbManagerFactory() {}

  /**
   * 전달된 (target) 클러스터 명과 테이블 명에 대한 CmdbManager 인스턴스를 반환. 동일한 조합에 대해서는 같은 인스턴스가 반환됨.
   *
   * @return CmdbManager
   */
  public static CmdbManager getInstance(IcebergSinkConfig config) {
    // Kakao CDC가 비활성화된 경우 no-op manager 반환
    if (!config.kakaoCdcEnabled()) {
      LOG.info("Kakao CDC is disabled, returning no-op CmdbManager");
      return new NoOpCmdbManager();
    }

    String clusterName = config.hadoopClusterName();
    String tableName = getTableName(config);
    String topicName = config.topics();

    if (clusterName == null || tableName == null || topicName == null) {
      throw new IllegalArgumentException("topic, clusterName, tableName must not be null");
    }

    // 캐시 키 생성
    String cacheKey = createCacheKey(topicName, clusterName, tableName);

    return INSTANCES.computeIfAbsent(
        cacheKey,
        key -> {
          LOG.info("Creating new CmdbManager instance for: {}", cacheKey);
          return new CmdbManager(config);
        });
  }

  /**
   * 캐시 키를 생성한다.
   *
   * @param topic 토픽 명. source DB ID, schema, table 정보를 가진다 ex) "limtan-my-s104-db1-tb1"
   * @param clusterName 목적지 하둡 클러스터 명
   * @param tableName 커넥터 설정의 iceberg.tables 으로, schema 정보까지 포함한다. ex) "zeroetl.table1"
   * @return 생성된 캐시 키
   */
  private static String createCacheKey(String topic, String clusterName, String tableName) {
    return topic + " - " + clusterName + " - " + tableName;
  }

  private static String getTableName(IcebergSinkConfig config) {
    String tableName = null;
    if (config.tables() != null && !config.tables().isEmpty()) {
      tableName = config.tables().get(0);
    }

    return tableName;
  }

  public static void removeInstance(IcebergSinkConfig config) {
    String cacheKey =
        createCacheKey(config.topics(), config.hadoopClusterName(), getTableName(config));

    CmdbManager removed = INSTANCES.remove(cacheKey);

    if (removed != null) {
      LOG.info("Removed CmdbManager instance for: {}", cacheKey);
    }
  }

  public static void clearCaches(IcebergSinkConfig config) {
    String cacheKey =
        createCacheKey(config.topics(), config.hadoopClusterName(), getTableName(config));

    CmdbManager manager = INSTANCES.get(cacheKey);
    if (manager != null) {
      manager.clearCaches();
      LOG.info("Cleared CmdbManager cache for: {}", cacheKey);
    } else {
      LOG.debug("No CmdbManager instance found to clear for: {}", cacheKey);
    }
  }
}
