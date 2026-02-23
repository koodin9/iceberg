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

import java.util.List;
import javax.management.MBeanServer;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MBeanRegisterFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(MBeanRegisterFactory.class);

  private MBeanRegisterFactory() {}

  public static ProcessCustomMetrics initializeProcessLatencyMBeans(
      MBeanServer mBeanServer, String connectorName) {
    String taskId = getTaskId();
    return new ProcessCustomMetrics(connectorName, taskId, mBeanServer);
  }

  public static PendingRecordCustomMetrics initializePendingRecordMBeans(
      MBeanServer mBeanServer, String connectorName) {
    String taskId = getTaskId();
    return new PendingRecordCustomMetrics(connectorName, taskId, mBeanServer);
  }

  private static String getTaskId() {
    String threadName = Thread.currentThread().getName();
    if (!threadName.contains("task-")) {
      LOGGER.warn("Illegal Thread Name = {}", threadName);
    }

    List<String> parts = Splitter.on("-").splitToList(threadName);
    return parts.get(parts.size() - 1);
  }
}
