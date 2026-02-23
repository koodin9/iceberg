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
package org.apache.iceberg.connect.metrics;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessCustomMetrics implements ProcessCustomMetricsMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessCustomMetrics.class);

  private final String connectorName;
  private final String taskId;
  private final MBeanServer mBeanServer;

  private final AtomicLong lastDmlLatency = new AtomicLong(0);
  private final LongAdder totalDmlLatency = new LongAdder();
  private final LongAdder pollCount = new LongAdder();

  private final AtomicLong lastDdlLatency = new AtomicLong(0);

  public ProcessCustomMetrics(String connectorName, String taskId, MBeanServer mBeanServer) {
    this.connectorName = connectorName;
    this.taskId = taskId;
    this.mBeanServer = mBeanServer;
  }

  public void recordDmlProcessLatency(List<SinkRecord> records, long currentMs) {
    if (records.isEmpty()) {
      return;
    }

    Long minSourceEventTime = getSourceTimestamp(records.get(0), "$__source");

    if (minSourceEventTime == null) {
      LOGGER.warn("min source event time is null");
      return;
    }

    if (currentMs < minSourceEventTime) {
      LOGGER.warn(
          "dml metric error = min source event time({}} is bigger than current time({})",
          minSourceEventTime,
          currentMs);
      return;
    }

    long latencyMs = currentMs - minSourceEventTime;
    LOGGER.debug(
        "currentMs = {} and minSourceEventTime = {}, currentMs - minSourceEventTime = {}",
        currentMs,
        minSourceEventTime,
        latencyMs);

    lastDmlLatency.set(latencyMs);
    totalDmlLatency.add(latencyMs);
    pollCount.increment();
  }

  public void recordDmlProcessLatency(Collection<SinkRecord> records, long currentMs) {
    recordDmlProcessLatency(records.stream().toList(), currentMs);
  }

  public void recordDdlProcessLatency(SinkRecord record, long currentMs) {
    LOGGER.info("ddl record = {}", record);
    Long sourceDdlTime = this.getSourceTimestamp(record, "source");

    if (sourceDdlTime == null) {
      LOGGER.warn("source ddl event time is null");
      return;
    }

    if (currentMs < sourceDdlTime) {
      LOGGER.warn(
          "ddl metric error = source ddl event time({}} is bigger than current time({})",
          sourceDdlTime,
          currentMs);
      return;
    }

    long latencyMs = currentMs - sourceDdlTime;
    LOGGER.debug(
        "currentMs = {} and sourceDdlEventTime = {}, currentMs - sourceDdlEventTime = {}",
        currentMs,
        sourceDdlTime,
        latencyMs);

    lastDdlLatency.set(latencyMs);
  }

  private Long getSourceTimestamp(SinkRecord record, String sourceField) {
    Object value = record.value();
    if (!(value instanceof Struct valueStruct)) {
      LOGGER.warn("sink schema value = {}", value);
      return null;
    }

    if (valueStruct.schema().field(sourceField) == null) {
      LOGGER.warn("valueStruct = {}", valueStruct);
      LOGGER.warn("valueStruct schema = {}", valueStruct.schema());
      return null;
    }

    Struct sourceStruct = valueStruct.getStruct(sourceField);
    LOGGER.debug("sink schema sourceStruct = {}", sourceStruct);
    if (sourceStruct == null || sourceStruct.schema().field("ts_ms") == null) {
      return null;
    }

    Field tsMsField = sourceStruct.schema().field("ts_ms");
    Long tsMs;

    LOGGER.debug("source tsMsField = {}", tsMsField);
    if (tsMsField.schema().type() == Schema.Type.INT32) {
      Integer tsMsInteger = sourceStruct.getInt32("ts_ms");
      tsMs = tsMsInteger == null ? null : tsMsInteger.longValue();
    } else {
      tsMs = sourceStruct.getInt64("ts_ms");
    }

    LOGGER.debug("source event time in milli sec= {}", tsMs);

    return tsMs;
  }

  public void registerProcessMBean() {
    try {
      ObjectName mbeanName = createProcessMBeanName();
      if (!mBeanServer.isRegistered(mbeanName)) {
        mBeanServer.registerMBean(this, mbeanName);
        LOGGER.info("Registered process record metric MBean {}", mbeanName);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to register ProcessLatency MBean for taskId: {}", taskId, e);
      throw new RuntimeException(e);
    }
  }

  public void unregisterProcessMBean() {
    try {
      ObjectName mbeanName = createProcessMBeanName();
      if (mBeanServer.isRegistered(mbeanName)) {
        mBeanServer.unregisterMBean(mbeanName);
        LOGGER.info("Unregistered process record metric MBean {}", mbeanName);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to unregister ProcessLatency MBean for taskId: {}", taskId, e);
    }
  }

  private ObjectName createProcessMBeanName() {
    String name =
        String.format(
            "custom.metrics:type=process-latency,connector=%s,task=%s", connectorName, taskId);
    try {
      return new ObjectName(name);
    } catch (MalformedObjectNameException e) {
      LOGGER.error("Failed to register ProcessLatency MBean for taskId: {}", taskId, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getLastDmlLatencyMillis() {
    return lastDmlLatency.get();
  }

  @Override
  public double getAvgDmlLatencyMillis() {
    long count = pollCount.sum();
    return (count == 0) ? 0.0 : (double) totalDmlLatency.sum() / count;
  }

  @Override
  public long getPollCount() {
    return pollCount.sum();
  }

  @Override
  public long getLastDdlExecutionsLatencyMillis() {
    return lastDdlLatency.get();
  }
}
