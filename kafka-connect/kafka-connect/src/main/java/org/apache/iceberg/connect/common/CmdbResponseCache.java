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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cmdb API 응답을 기록 하기 위한 thread-safe 캐시.
 *
 * <ul>
 *   <li>key: ddl version
 *   <li>value: {@code DdlExecutionResponse} or {@code List<DmlStatusResponse>}
 * </ul>
 */
public class CmdbResponseCache<T> {

  private static final Logger LOG = LoggerFactory.getLogger(CmdbResponseCache.class);

  private final int maxSize;
  private final Map<Long, T> cache;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /** 기본 캐시 사이즈 = 3 */
  public CmdbResponseCache() {
    this(3);
  }

  public CmdbResponseCache(int maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("Cache size must be positive, but was: " + maxSize);
    }

    this.maxSize = maxSize;
    this.cache = new LinkedHashMap<>(maxSize);

    LOG.info("CmdbResponseCache initialized with maxSize: {}", maxSize);
  }

  public Optional<T> get(long key) {
    lock.readLock().lock();
    try {
      return Optional.ofNullable(cache.get(key));
    } finally {
      lock.readLock().unlock();
    }
  }

  public List<T> getAll() {
    lock.readLock().lock();
    try {
      return Lists.newArrayList(cache.values());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * 캐시 크기가 지정된 사이즈 초과시, 가장 작은 키값 항목을 제거
   *
   * <p>value 는 null 일 수 있으며, 이는 API 호출 결과가 없었음을 나타냄
   */
  public void put(long key, T value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null");
    }

    lock.writeLock().lock();
    try {
      LOG.info("[CMDB Cache] put: key={}, value={}", key, value);
      T previousValue = cache.put(key, value);

      // 캐시 크기가 최대값 초과시
      if (cache.size() > maxSize) {
        Long keyToRemove =
            cache.keySet().stream()
                .filter(k -> !k.equals(key)) // 방금 추가된 키 제외
                .min(Long::compareTo)
                .orElse(null);

        if (keyToRemove != null) {
          cache.remove(keyToRemove);
          LOG.debug("[CMDB Cache] Remove cache entry on {}, for new entry of {}", keyToRemove, key);
        }
      }

      if (previousValue != null) {
        LOG.debug("[CMDB Cache] Updated cache entry for key: {}", key);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() {
    lock.writeLock().lock();
    try {
      int sizeBefore = cache.size();
      cache.clear();
      LOG.debug("[CMDB Cache] Cache clear complete. removed {} entries", sizeBefore);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public int size() {
    lock.readLock().lock();
    try {
      return cache.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean isEmpty() {
    lock.readLock().lock();
    try {
      return cache.isEmpty();
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean containsKey(long key) {
    lock.readLock().lock();
    try {
      return cache.containsKey(key);
    } finally {
      lock.readLock().unlock();
    }
  }

  public int getMaxSize() {
    return maxSize;
  }
}
