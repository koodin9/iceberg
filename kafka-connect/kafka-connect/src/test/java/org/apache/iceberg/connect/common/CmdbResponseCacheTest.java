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
package org.apache.iceberg.connect.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.iceberg.connect.cmdb.dto.ddl.response.DdlExecutionResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CmdbResponseCacheTest {

  private CmdbResponseCache<DdlExecutionResponse> cache;
  private DdlExecutionResponse mockResponse1;
  private DdlExecutionResponse mockResponse2;
  private DdlExecutionResponse mockResponse3;
  private DdlExecutionResponse mockResponse4;

  @BeforeEach
  void before() {
    cache = new CmdbResponseCache<>();

    // Mock 객체 생성
    mockResponse1 = mock(DdlExecutionResponse.class);
    when(mockResponse1.getDdlVersion()).thenReturn(1L);

    mockResponse2 = mock(DdlExecutionResponse.class);
    when(mockResponse2.getDdlVersion()).thenReturn(2L);

    mockResponse3 = mock(DdlExecutionResponse.class);
    when(mockResponse3.getDdlVersion()).thenReturn(3L);

    mockResponse4 = mock(DdlExecutionResponse.class);
    when(mockResponse4.getDdlVersion()).thenReturn(4L);
  }

  @Test
  void testDefaultConstructor() {
    CmdbResponseCache<DdlExecutionResponse> defaultCache = new CmdbResponseCache<>();
    assertEquals(3, defaultCache.getMaxSize());
    assertTrue(defaultCache.isEmpty());
  }

  @Test
  void testCustomSizeConstructor() {
    CmdbResponseCache<DdlExecutionResponse> customCache = new CmdbResponseCache<>(5);
    assertEquals(5, customCache.getMaxSize());
    assertTrue(customCache.isEmpty());
  }

  @Test
  void testInvalidSizeConstructor() {
    assertThrows(IllegalArgumentException.class, () -> new CmdbResponseCache<>(0));
    assertThrows(IllegalArgumentException.class, () -> new CmdbResponseCache<>(-1));
  }

  @Test
  void testPutAndGet() {
    // 캐시에 저장
    cache.put(mockResponse1.getDdlVersion(), mockResponse1);

    Optional<DdlExecutionResponse> result = cache.get(1L);
    assertTrue(result.isPresent());
    assertEquals(mockResponse1, result.get());
    assertEquals(1, cache.size());
  }

  @Test
  void testGetNotExists() {
    Optional<DdlExecutionResponse> result = cache.get(999L);
    assertFalse(result.isPresent());
  }

  @Test
  void testPutNullThrowsException() {
    assertThrows(IllegalArgumentException.class, () -> cache.put(999L, null));
  }

  @Test
  void testUpdate() {
    cache.put(mockResponse1.getDdlVersion(), mockResponse1);
    assertEquals(1, cache.size());

    // 동일한 DDL 버전으로 다른 객체 저장 (업데이트)
    DdlExecutionResponse updatedResponse = mock(DdlExecutionResponse.class);
    when(updatedResponse.getDdlVersion()).thenReturn(1L);
    cache.put(updatedResponse.getDdlVersion(), updatedResponse);

    // 크기는 여전히 1이어야 함
    assertEquals(1, cache.size());

    // 새로운 객체가 저장되었는지 확인
    Optional<DdlExecutionResponse> result = cache.get(1L);
    assertTrue(result.isPresent());
    assertEquals(updatedResponse, result.get());
  }

  @Test
  void testEviction() {
    // 캐시 최대 크기가 3이므로 4개 저장 시 가장 오래된 것이 제거되어야 함
    cache.put(mockResponse1.getDdlVersion(), mockResponse1);
    cache.put(mockResponse2.getDdlVersion(), mockResponse2);
    cache.put(mockResponse3.getDdlVersion(), mockResponse3);

    assertEquals(3, cache.size());
    assertTrue(cache.containsKey(1L));
    assertTrue(cache.containsKey(2L));
    assertTrue(cache.containsKey(3L));

    // 4번째 항목 추가 - 1번이 제거되어야 함
    cache.put(mockResponse4.getDdlVersion(), mockResponse4);

    assertEquals(3, cache.size());
    assertFalse(cache.containsKey(1L)); // 가장 오래된 항목 제거됨
    assertTrue(cache.containsKey(2L));
    assertTrue(cache.containsKey(3L));
    assertTrue(cache.containsKey(4L));
  }

  @Test
  void testConcurrency() throws InterruptedException {
    // 간단한 동시성 테스트
    int numThreads = 10;
    Thread[] threads = new Thread[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int threadIndex = i;
      threads[i] =
          new Thread(
              () -> {
                DdlExecutionResponse response = mock(DdlExecutionResponse.class);
                when(response.getDdlVersion()).thenReturn((long) threadIndex);
                cache.put(response.getDdlVersion(), response);
                cache.get(threadIndex);
              });
    }

    // 모든 스레드 시작
    for (Thread thread : threads) {
      thread.start();
    }

    // 모든 스레드 완료 대기
    for (Thread thread : threads) {
      thread.join();
    }

    // 캐시 크기는 최대 크기를 초과하지 않아야 함
    assertTrue(cache.size() <= cache.getMaxSize());
  }
}
