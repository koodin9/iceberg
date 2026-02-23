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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.cmdb.dto.ddl.response.DdlExecutionResponse;
import org.apache.iceberg.connect.cmdb.dto.dml.response.DmlStatusResponse;
import org.apache.iceberg.connect.cmdb.model.DdlStatus;
import org.apache.iceberg.connect.cmdb.model.PartitionStatus;
import org.apache.iceberg.connect.simpleserver.TestApiServer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;

public class TestContext {
  private final Logger logger = LoggerFactory.getLogger(TestContext.class);

  private static volatile TestContext instance;

  public static final ObjectMapper MAPPER = new ObjectMapper();
  public static final int CONNECT_PORT = 8083;

  private static final int MINIO_PORT = 9000;
  private static final int CATALOG_PORT = 8181;
  private static final String BOOTSTRAP_SERVERS = "localhost:29092";
  private static final int API_SERVER_PORT = 8080;
  private static final String AWS_ACCESS_KEY = "minioadmin";
  private static final String AWS_SECRET_KEY = "minioadmin";
  private static final String AWS_REGION = "us-east-1";

  private final TestApiServer localApiServer;

  public static synchronized TestContext instance() {
    if (instance == null) {
      instance = new TestContext();
    }
    return instance;
  }

  private TestContext() {
    // Start API server after Kafka Connect is ready
    localApiServer = new TestApiServer(API_SERVER_PORT);
    try {
      localApiServer.start();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start API server", e);
    }

    File logDir = new File("build/testlogs");
    logDir.mkdirs();

    ComposeContainer container =
        new ComposeContainer(new File("./docker/docker-compose.yml"))
            .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
            .withEnv(
                "CONNECT_LOG4J_APPENDER_STDOUT_LAYOUT_CONVERSIONPATTERN",
                "[%d] %p %X{connector.context} %c: %m%n")
            .withLogConsumer("connect", outputFrame -> appendLog(logDir, "connect", outputFrame))
            .withStartupTimeout(Duration.ofMinutes(2))
            .waitingFor("connect", Wait.forHttp("/connectors"));
    container.start();
  }

  public void startConnector(KafkaConnectUtils.Config config) {
    KafkaConnectUtils.startConnector(config);
    KafkaConnectUtils.ensureConnectorRunning(config.getName());
  }

  public void stopConnector(String name) {
    KafkaConnectUtils.stopConnector(name);
  }

  public Catalog initLocalCatalog() {
    String localCatalogUri = "http://localhost:" + CATALOG_PORT;
    RESTCatalog result = new RESTCatalog();
    result.initialize(
        "local",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, localCatalogUri)
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO")
            .put("s3.endpoint", "http://localhost:" + MINIO_PORT)
            .put("s3.access-key-id", AWS_ACCESS_KEY)
            .put("s3.secret-access-key", AWS_SECRET_KEY)
            .put("s3.path-style-access", "true")
            .put("client.region", AWS_REGION)
            .build());
    return result;
  }

  public Map<String, Object> connectorCatalogProperties() {
    return ImmutableMap.<String, Object>builder()
        .put(
            "iceberg.catalog." + CatalogUtil.ICEBERG_CATALOG_TYPE,
            CatalogUtil.ICEBERG_CATALOG_TYPE_REST)
        .put("iceberg.catalog." + CatalogProperties.URI, "http://iceberg:" + CATALOG_PORT)
        .put(
            "iceberg.catalog." + CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.aws.s3.S3FileIO")
        .put("iceberg.catalog.s3.endpoint", "http://minio:" + MINIO_PORT)
        .put("iceberg.catalog.s3.access-key-id", AWS_ACCESS_KEY)
        .put("iceberg.catalog.s3.secret-access-key", AWS_SECRET_KEY)
        .put("iceberg.catalog.s3.path-style-access", true)
        .put("iceberg.catalog.client.region", AWS_REGION)
        .build();
  }

  public KafkaProducer<String, String> initLocalProducer() {
    return new KafkaProducer<>(
        ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            BOOTSTRAP_SERVERS,
            ProducerConfig.CLIENT_ID_CONFIG,
            UUID.randomUUID().toString()),
        new StringSerializer(),
        new StringSerializer());
  }

  public Admin initLocalAdmin() {
    return Admin.create(
        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS));
  }

  private void appendLog(File logDir, String serviceName, OutputFrame frame) {
    String log = frame.getUtf8String();
    if (log == null || log.isEmpty()) {
      return;
    }
    File logFile = new File(logDir, serviceName + ".log");
    try (PrintWriter writer = new PrintWriter(new FileWriter(logFile, true))) {
      writer.print(log);
    } catch (IOException e) {
      logger.warn("Failed to write container log for {}", serviceName, e);
    }
  }

  public String getApiServerUrlForContainer() {
    return "http://host.docker.internal:" + API_SERVER_PORT;
  }

  public TestApiServer getApiServer() {
    return localApiServer;
  }

  public void initializeDdlExecution(
      long id,
      long ddlVersion,
      String ddlStatement,
      String resultMsg,
      String dbName,
      String tableName) {
    getApiServer()
        .initializeDdlExecutions(
            new DdlExecutionResponse(
                id,
                "test-cluster",
                getFullTableName(dbName, tableName),
                ddlVersion,
                ddlStatement,
                ddlStatement,
                DdlStatus.PROCESSED,
                resultMsg,
                null,
                "",
                ""));
  }

  public String getFullTableName(String dbName, String tableName) {
    return String.format("%s.%s", dbName, tableName);
  }

  /**
   * Initialize DML consumer status for testing DDL processing. The lastProcessedDmlId should match
   * the last_dml_info in the DDL event header.
   *
   * @param partitionId the Kafka partition ID
   * @param ddlVersion the DDL version this DML status belongs to
   * @param gtid the GTID of the last processed DML
   * @param pos the binlog position of the last processed DML
   * @param row the row number of the last processed DML
   * @param dbName the database name
   * @param tableName the table name
   */
  public void initializeDmlStatus(
      int partitionId,
      long ddlVersion,
      String gtid,
      String pos,
      String row,
      String dbName,
      String tableName) {
    String lastProcessedDmlId =
        String.format("{\"gtid\":\"%s\",\"pos\":\"%s\",\"row\":\"%s\"}", gtid, pos, row);
    getApiServer()
        .initializeDmlStatuses(
            new DmlStatusResponse(
                partitionId, // id
                "test-cluster",
                getFullTableName(dbName, tableName),
                partitionId,
                0L, // offset
                ddlVersion,
                lastProcessedDmlId,
                PartitionStatus.DONE,
                "",
                ""));
  }
}
