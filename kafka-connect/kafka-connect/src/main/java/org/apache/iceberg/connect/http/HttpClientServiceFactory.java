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
package org.apache.iceberg.connect.http;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpClientService 인스턴스를 URL별로 관리하는 팩토리 클래스
 *
 * <p>고유한 (baseUrl, token) 조합에 대해 하나의 HttpClientService 인스턴스만 생성
 */
public class HttpClientServiceFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HttpClientServiceFactory.class);

  // 전역 공유 Executor - 모든 HttpClientService 인스턴스가 공유
  private static final ExecutorService SHARED_EXECUTOR = createSharedExecutor();

  // URL과 토큰 조합별로 HttpClientService 인스턴스를 캐시
  private static final Map<String, HttpClientService> INSTANCES = Maps.newConcurrentMap();

  private HttpClientServiceFactory() {}

  private static ExecutorService createSharedExecutor() {
    // IO 바운드 작업에 적합한 스레드 풀 크기 설정
    int threadCount = Math.max(8, Runtime.getRuntime().availableProcessors());

    LOG.info("🌐 Creating shared HTTP executor with {} threads", threadCount);

    return Executors.newFixedThreadPool(
        threadCount,
        r -> {
          Thread thread = new Thread(r, "shared-http-client-");
          thread.setDaemon(true);
          return thread;
        });
  }

  public static HttpClientService getInstance(String baseUrl) {
    return getInstance(baseUrl, null);
  }

  /**
   * 전달된 baseUrl과 token에 대한 HttpClientService 인스턴스를 반환. 동일한 조합에 대해서는 같은 인스턴스가 반환됨. 단, token은 값 유무만
   * 따짐. (key 생성과정 참조)
   *
   * @param baseUrl API 기본 URL
   * @param token 인증 토큰 (nullable)
   * @return HttpClientService 인스턴스
   */
  public static HttpClientService getInstance(String baseUrl, String token) {
    if (baseUrl == null) {
      throw new IllegalArgumentException("baseUrl cannot be null");
    }

    return INSTANCES.computeIfAbsent(
        baseUrl,
        key -> {
          LOG.info("🌐 Creating new HttpClientService instance for: {}", baseUrl);
          return new HttpClientService(baseUrl, token, SHARED_EXECUTOR);
        });
  }
}
