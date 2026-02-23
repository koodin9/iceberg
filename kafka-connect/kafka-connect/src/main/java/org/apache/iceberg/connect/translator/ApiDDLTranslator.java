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
package org.apache.iceberg.connect.translator;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.connect.http.HttpClientService;
import org.apache.iceberg.connect.http.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiDDLTranslator implements DDLTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(ApiDDLTranslator.class);
  private final HttpClientService httpClientService;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final String TRANSLATE_ENDPOINT = "/api/v1/ddl/translate/iceberg";

  public ApiDDLTranslator(HttpClientService httpClientService) {
    this.httpClientService = httpClientService;
  }

  @Override
  public TranslationResponse translate(
      String sourceDB,
      String targetDB,
      String targetTableName,
      String ddlStatement,
      Set<DDLType> skipDDLTypes,
      Boolean useDatetimeAsTimestamptz)
      throws DDLTranslationException {

    try {
      String requestBody =
          objectMapper.writeValueAsString(
              Map.of(
                  "sourceDB", sourceDB,
                  "targetDB", targetDB,
                  "targetTableName", targetTableName,
                  "ddlStatement", ddlStatement,
                  "skipDDLTypes", skipDDLTypes,
                  "useDatetimeAsTimestamptz", useDatetimeAsTimestamptz));

      LOG.info("ddl translator request: {}", requestBody);

      HttpResponse<String> response = httpClientService.sendPost(TRANSLATE_ENDPOINT, requestBody);

      if (response.statusCode() == HttpURLConnection.HTTP_OK) {
        LOG.info("ddl translator response: {}", response.body());
        return objectMapper.readValue(response.body(), TranslationResponse.class);
      } else {
        handleErrorResponse(response);
        return null;
      }
    } catch (HttpException e) {
      throw new DDLTranslationException("Translation failed due to HTTP error", e);
    } catch (IOException e) {
      LOG.error("Error translating DDL", e);
      throw new DDLTranslationException("Error translating DDL", e);
    }
  }

  private void handleErrorResponse(HttpResponse<String> response) throws IOException {
    ErrorResponse errorResponse = objectMapper.readValue(response.body(), ErrorResponse.class);
    String errorMessage = errorResponse.getMessage();
    String errorCode = errorResponse.getCode();

    switch (errorCode) {
      case "TRANSLATION_FAILURE":
        throw new DDLTranslationException("Translation failed: " + errorMessage, null);
      case "UNSUPPORTED_DATABASE_COMBINATION":
        throw new DDLTranslationException(
            "Unsupported database combination: " + errorMessage, null);
      default:
        throw new DDLTranslationException("Unknown error: " + errorMessage, null);
    }
  }
}
