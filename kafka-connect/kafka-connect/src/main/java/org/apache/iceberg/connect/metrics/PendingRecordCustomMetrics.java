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
import java.util.Locale;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PendingRecordCustomMetrics {

  private static final Logger LOGGER = LoggerFactory.getLogger(PendingRecordCustomMetrics.class);

  private final Map<Integer, PartitionPendingRecordMetrics> metricsMap = Maps.newConcurrentMap();

  private final String connectorName;
  private final String taskId;
  private final MBeanServer mBeanServer;

  public PendingRecordCustomMetrics(String connectorName, String taskId, MBeanServer mBeanServer) {
    this.connectorName = connectorName;
    this.taskId = taskId;
    this.mBeanServer = mBeanServer;
  }

  public void updatePendingRecordOffsets(Integer assignedPartition, Long offset) {
    PartitionPendingRecordMetrics partitionPendingRecordMetrics = metricsMap.get(assignedPartition);
    partitionPendingRecordMetrics.updatePendingRecordOffset(offset);
  }

  public void registerPartitionMBean(Collection<TopicPartition> partitions) {
    for (TopicPartition topicPartition : partitions) {
      try {
        ObjectName mbeanName = createPartitionMBeanName(topicPartition.partition());
        PartitionPendingRecordMetrics partitionPendingRecordMetrics =
            metricsMap.computeIfAbsent(
                topicPartition.partition(), p -> new PartitionPendingRecordMetrics());
        if (!mBeanServer.isRegistered(mbeanName)) {
          mBeanServer.registerMBean(partitionPendingRecordMetrics, mbeanName);
          LOGGER.info("Registered pending record metric MBean {}", mbeanName);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to register PendingRecord MBean for partition: {}", partitions, e);
        throw new RuntimeException(e);
      }
    }
  }

  public void unregisterPartitionMBean(Collection<TopicPartition> partitions) {
    for (TopicPartition topicPartition : partitions) {
      try {
        ObjectName mbeanName = createPartitionMBeanName(topicPartition.partition());
        if (mBeanServer.isRegistered(mbeanName)) {
          mBeanServer.unregisterMBean(mbeanName);
          LOGGER.info("Unregistered pending record metric MBean {}", mbeanName);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to unregister PendingRecord MBean for partition: {}", partitions, e);
      }
    }
  }

  private ObjectName createPartitionMBeanName(int partition) {
    String name =
        String.format(
            Locale.ROOT,
            "custom.metrics:type=pending-record,connector=%s,task=%s,partition=%d",
            connectorName,
            taskId,
            partition);
    try {
      return new ObjectName(name);
    } catch (MalformedObjectNameException e) {
      LOGGER.error("Failed to register PendingRecord MBean for partition: {}", partition, e);
      throw new RuntimeException(e);
    }
  }
}
