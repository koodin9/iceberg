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

import static org.apache.iceberg.connect.simpleserver.Utils.readJsonBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.connect.translator.ColumnDetail;
import org.apache.iceberg.connect.translator.DDL;
import org.apache.iceberg.connect.translator.DDLType;
import org.apache.iceberg.connect.translator.TranslationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DDL translate API 핸들러 (테스트용) */
public class DdlTranslateHandler extends HttpServlet {
  private final Logger logger = LoggerFactory.getLogger(DdlTranslateHandler.class);
  private final ObjectMapper mapper = new ObjectMapper();

  // DDL 패턴 정규식
  private static final Pattern ADD_COLUMN_PATTERN =
      Pattern.compile(
          "alter\\s+table\\s+\\w+\\s+add\\s+(?:column\\s+)?(\\w+)\\s+(\\w+)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern DROP_COLUMN_PATTERN =
      Pattern.compile(
          "alter\\s+table\\s+\\w+\\s+drop\\s+(?:column\\s+)?(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern MODIFY_COLUMN_PATTERN =
      Pattern.compile(
          "alter\\s+table\\s+\\w+\\s+modify\\s+(?:column\\s+)?(\\w+)\\s+(\\w+)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern RENAME_COLUMN_PATTERN =
      Pattern.compile(
          "alter\\s+table\\s+\\w+\\s+rename\\s+column\\s+(\\w+)\\s+to\\s+(\\w+)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern TRUNCATE_PATTERN =
      Pattern.compile("truncate\\s+(?:table\\s+)?\\w+", Pattern.CASE_INSENSITIVE);

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

    String jsonBody = readJsonBody(req);
    logger.info("[DDL Translate] POST request received: {}", jsonBody);

    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> request = mapper.readValue(jsonBody, Map.class);
      String ddlStatement = (String) request.get("ddlStatement");

      DDL[] translatedDDLs;
      String message = "success";

      if (ddlStatement == null) {
        translatedDDLs = new DDL[] {};
        message = "No DDL statement provided";
      } else {
        translatedDDLs = parseDDL(ddlStatement);
        if (translatedDDLs.length == 0) {
          message = "No translation needed";
        }
      }

      TranslationResponse response = new TranslationResponse(translatedDDLs, message);

      resp.setContentType("application/json");
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write(mapper.writeValueAsString(response));

    } catch (Exception e) {
      logger.error("Error processing DDL translate request", e);
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("{\"error\": \"Invalid request\"}");
    }
  }

  // 간단한 DDL Translator 구현
  private DDL[] parseDDL(String ddlStatement) {
    // TRUNCATE
    if (TRUNCATE_PATTERN.matcher(ddlStatement).find()) {
      logger.info("[DDL Translate] Parsed TRUNCATE DDL");
      return new DDL[] {new DDL(DDLType.TRUNCATE, null, null, null, null, null)};
    }

    // ADD COLUMN
    Matcher addMatcher = ADD_COLUMN_PATTERN.matcher(ddlStatement);
    if (addMatcher.find()) {
      String columnName = addMatcher.group(1);
      String columnType = addMatcher.group(2);
      logger.info("[DDL Translate] Parsed ADD_COLUMN: {} {}", columnName, columnType);
      return new DDL[] {
        new DDL(
            DDLType.ADD_COLUMN,
            null,
            columnName,
            null,
            null,
            new ColumnDetail(columnType, true, null))
      };
    }

    // DROP COLUMN
    Matcher dropMatcher = DROP_COLUMN_PATTERN.matcher(ddlStatement);
    if (dropMatcher.find()) {
      String columnName = dropMatcher.group(1);
      logger.info("[DDL Translate] Parsed DROP_COLUMN: {}", columnName);
      return new DDL[] {new DDL(DDLType.DROP_COLUMN, null, columnName, null, null, null)};
    }

    // MODIFY COLUMN
    Matcher modifyMatcher = MODIFY_COLUMN_PATTERN.matcher(ddlStatement);
    if (modifyMatcher.find()) {
      String columnName = modifyMatcher.group(1);
      String columnType = modifyMatcher.group(2);
      logger.info("[DDL Translate] Parsed MODIFY_COLUMN: {} {}", columnName, columnType);
      return new DDL[] {
        new DDL(
            DDLType.MODIFY_COLUMN,
            null,
            columnName,
            null,
            null,
            new ColumnDetail(columnType, true, null))
      };
    }

    // RENAME COLUMN
    Matcher renameMatcher = RENAME_COLUMN_PATTERN.matcher(ddlStatement);
    if (renameMatcher.find()) {
      String oldName = renameMatcher.group(1);
      String newName = renameMatcher.group(2);
      logger.info("[DDL Translate] Parsed RENAME_COLUMN: {} -> {}", oldName, newName);
      return new DDL[] {new DDL(DDLType.RENAME_COLUMN, null, oldName, null, newName, null)};
    }

    logger.info("[DDL Translate] No matching DDL pattern for: {}", ddlStatement);
    return new DDL[] {};
  }
}
