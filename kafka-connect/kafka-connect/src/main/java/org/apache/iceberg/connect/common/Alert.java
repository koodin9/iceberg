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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.http.HttpClientService;
import org.apache.iceberg.connect.http.HttpClientServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class Alert {

  /**
   * alertType 필드는 현재 항상 null 로 보내고 있다. connect-admin 쪽에서 여러 타입에 맞게 메세지 포메팅을 해주려는 계획 때문에 존재하지만 현재로서는
   * "ddl" 타입만 존재하고 항상 같은 포멧으로 발송함.
   */
  public record Body(
      String message,
      @JsonProperty("alert_target") String[] alertTarget,
      @JsonProperty("alert_type") String alertType) {}

  private static final Logger LOG = LoggerFactory.getLogger(Alert.class.getName());
  private static final String PATH_PREFIX = "/api/v1/notification/iceberg/sink/alert/";

  private final HttpClientService client;
  private final IcebergSinkConfig config;
  private final ObjectMapper mapper;

  public Alert(IcebergSinkConfig config) {
    this.config = config;
    this.client = HttpClientServiceFactory.getInstance(config.connectAdminUrl());
    mapper = new ObjectMapper();
  }

  /**
   * 기본적으로 아래 포멧의 메세지가 발송된다. --- [🔉 ZeroETL 알림]
   *
   * <p>Source Cluster : chacha-cdctest-dbox (MySQL) Source Data Mapping : cdc_test.table_a Target
   * Cluster : hadoop-dev (Iceberg) Target Data Mapping : zeroetl.table_a_test
   *
   * <p>{message} ---
   */
  public void sendNotificationMessage(String message, AlertTarget target, Level severity) {
    String severityStr =
        switch (severity) {
          case ERROR -> "🚨";
          case WARN -> "⚠️";
          case INFO -> "ℹ️";
          case DEBUG -> "🐞";
          case TRACE -> "🔍";
        };

    Body body = new Body(String.join(" ", severityStr, message), target.toArray(), null);

    LOG.info("Try sending notification message: {}", body);

    try {
      String json = mapper.writeValueAsString(body);
      client.sendPost(PATH_PREFIX + config.connectorName(), json);
    } catch (Exception e) {
      LOG.error("Failed to send notification message", e);
    }
  }
}
