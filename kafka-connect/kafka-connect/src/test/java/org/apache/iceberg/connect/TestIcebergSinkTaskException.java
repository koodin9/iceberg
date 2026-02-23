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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import org.apache.iceberg.connect.common.Alert;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * IcebergSinkTask.put()의 예외 처리 테스트
 *
 * <p>RebalanceInProgressException이 RetriableException으로 변환되는지 검증
 */
public class TestIcebergSinkTaskException {

  private IcebergSinkTask sinkTask;
  private Committer mockCommitter;
  private Alert mockAlert;
  private SinkTaskContext mockContext;

  @BeforeEach
  void before() throws Exception {
    sinkTask = new IcebergSinkTask();
    mockCommitter = mock(Committer.class);
    mockAlert = mock(Alert.class);
    mockContext = mock(SinkTaskContext.class);

    // reflection으로 private 필드 설정
    setField(sinkTask, "committer", mockCommitter);
    setField(sinkTask, "alert", mockAlert);
    setField(sinkTask, "context", mockContext);
  }

  private void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = findField(target.getClass(), fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
    Class<?> current = clazz;
    while (current != null) {
      try {
        return current.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  @Test
  @DisplayName("RebalanceInProgressException은 RetriableException으로 변환되어야 함")
  void shouldConvertRebalanceExceptionToRetriableException() {
    // given
    Collection<SinkRecord> records = Collections.emptyList();
    RebalanceInProgressException rebalanceException =
        new RebalanceInProgressException("Offset commit cannot be completed");

    doThrow(rebalanceException).when(mockCommitter).save(any());

    // when & then
    assertThatThrownBy(() -> sinkTask.put(records))
        .isInstanceOf(RetriableException.class)
        .hasMessageContaining("Rebalance in progress")
        .hasCause(rebalanceException);
  }

  @Test
  @DisplayName("일반 RuntimeException은 RuntimeException으로 전파되어야 함")
  void shouldPropagateOtherRuntimeExceptionAsRuntimeException() {
    // given
    Collection<SinkRecord> records = Collections.emptyList();
    IllegalStateException otherException = new IllegalStateException("Some error");

    doThrow(otherException).when(mockCommitter).save(any());

    // when & then
    assertThatThrownBy(() -> sinkTask.put(records))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("레코드 처리 중 에러 발생");
  }

  @Test
  @DisplayName("IOException은 RuntimeException으로 감싸져야 함")
  void shouldWrapIOExceptionInRuntimeException() {
    // given
    Collection<SinkRecord> records = Collections.emptyList();
    RuntimeException wrappedException =
        new RuntimeException("IO failed", new java.io.IOException("Disk full"));

    doThrow(wrappedException).when(mockCommitter).save(any());

    // when & then
    assertThatThrownBy(() -> sinkTask.put(records))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("레코드 처리 중 에러 발생");
  }

  @Test
  @DisplayName("RetriableException은 Kafka Connect가 재시도할 수 있음을 나타냄")
  void retriableExceptionShouldBeRetryableByKafkaConnect() {
    // RetriableException은 Kafka Connect 프레임워크가 인식하는 예외
    // 이 예외가 발생하면 task를 죽이지 않고 재시도함
    RetriableException exception = new RetriableException("test");

    // RetriableException이 올바른 상속 구조를 가지는지 확인
    assertThat(exception).isInstanceOf(org.apache.kafka.connect.errors.ConnectException.class);
  }
}
