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
package org.apache.iceberg.connect.simpleserver;

import org.apache.iceberg.connect.cmdb.dto.ddl.response.DdlExecutionResponse;
import org.apache.iceberg.connect.cmdb.dto.dml.response.DmlStatusResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test 를 위해 실행되는 Simple HTTP API server kafka connect 가 호출할 외부 API 호출을 mocking 한다. */
public class TestApiServer {

  private static final Logger LOG = LoggerFactory.getLogger(TestApiServer.class);

  private final Server server;
  private final DdlExecutionHandler ddlExecutionHandler;
  private final DmlStatusHandler dmlStatusHandler;

  public TestApiServer(int port) {
    this.server = new Server(port);

    // Create handler instances first to allow external access and reuse
    this.ddlExecutionHandler = new DdlExecutionHandler();
    this.dmlStatusHandler = new DmlStatusHandler();

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");

    // Reuse the same handler instances for both path patterns
    ServletHolder ddlHolder = new ServletHolder(ddlExecutionHandler);
    ServletHolder dmlHolder = new ServletHolder(dmlStatusHandler);

    context.addServlet(ddlHolder, "/api/v2/cdc/icebergsinkddlexecutions");
    context.addServlet(ddlHolder, "/api/v2/cdc/icebergsinkddlexecutions/*");
    context.addServlet(dmlHolder, "/api/v2/cdc/icebergsinkdmlconsumerstatuses");
    context.addServlet(dmlHolder, "/api/v2/cdc/icebergsinkdmlconsumerstatuses/*");
    context.addServlet(new ServletHolder(new DdlTranslateHandler()), "/api/v1/ddl/translate/*");
    context.addServlet(
        new ServletHolder(new AlertHandler()), "/api/v1/notification/iceberg/sink/alert/*");
    server.setHandler(context);
  }

  /** Reset all handlers to empty state. */
  public void resetHandlers() {
    ddlExecutionHandler.reset();
    dmlStatusHandler.reset();
    LOG.info("All handlers reset");
  }

  /** Initialize DDL execution handler with test data. */
  public void initializeDdlExecutions(DdlExecutionResponse... initialData) {
    ddlExecutionHandler.initializeWith(initialData);
    LOG.info("DDL execution handler initialized with {} entries", initialData.length);
  }

  /** Initialize DML status handler with test data. */
  public void initializeDmlStatuses(DmlStatusResponse... initialData) {
    dmlStatusHandler.initializeWith(initialData);
    LOG.info("DML status handler initialized with {} entries", initialData.length);
  }

  public void start() throws Exception {
    server.start();
    LOG.info("🚀 Test API server started on port {}", server.getURI().getPort());
  }

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
      LOG.info("Test API server stopped");
    }
  }
}
