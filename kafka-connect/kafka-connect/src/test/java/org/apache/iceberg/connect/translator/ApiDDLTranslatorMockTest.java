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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.http.HttpResponse;
import java.util.Set;
import org.apache.iceberg.connect.http.HttpClientService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTests")
class ApiDDLTranslatorMockTest {

  private ApiDDLTranslator apiSQLTranslator;
  private HttpClientService httpClientService;

  @BeforeEach
  void before() {
    httpClientService = mock(HttpClientService.class);
    apiSQLTranslator = new ApiDDLTranslator(httpClientService);
  }

  @Test
  void testTranslate_success() throws Exception {
    TranslationResponse mockResponse =
        loadMockResponse("addResponse.json", TranslationResponse.class);
    mockHttpClientService(mockResponse, 200, false);

    TranslationResponse result =
        apiSQLTranslator.translate(
            "mysql",
            "iceberg",
            "cdc_test_table",
            "ALTER TABLE cdc_test_table ADD COLUMN new_column INT",
            Set.of(DDLType.INDEX),
            false);

    assertNotNull(result);
    DDL expected =
        new DDL(
            DDLType.ADD_COLUMN,
            "cdc_test_table",
            "new_column",
            null,
            null,
            new ColumnDetail("int", true, null));
    assertEquals(expected, result.translatedDDLs()[0]);
  }

  @Test
  void testTranslate_modifyResponse() throws Exception {
    TranslationResponse mockResponse =
        loadMockResponse("modifyResponse.json", TranslationResponse.class);
    mockHttpClientService(mockResponse, 200, false);

    TranslationResponse result =
        apiSQLTranslator.translate(
            "mysql",
            "iceberg",
            "cdc_test_table",
            "ALTER TABLE cdc_test_table MODIFY COLUMN t_tmp INT",
            Set.of(DDLType.INDEX),
            false);

    assertNotNull(result);
    DDL expected =
        new DDL(
            DDLType.MODIFY_COLUMN,
            "cdc_test_table",
            "t_tmp",
            null,
            null,
            new ColumnDetail("int", true, null));
    assertEquals(expected, result.translatedDDLs()[0]);
  }

  @Test
  void testTranslate_unsupportedDatabaseCombination() throws Exception {
    ErrorResponse mockResponse =
        loadMockResponse("unsupportedDatabaseResponse.json", ErrorResponse.class);
    mockHttpClientService(mockResponse, 400, true);

    assertThrows(
        DDLTranslationException.class,
        () -> {
          apiSQLTranslator.translate(
              "unsupported_db",
              "iceberg",
              "cdc_test_table",
              "ALTER TABLE cdc_test_table ADD COLUMN new_column INT",
              Set.of(DDLType.INDEX),
              false);
        });
  }

  private <T> T loadMockResponse(String fileName, Class<T> responseType) throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(
        new File("src/test/resources/fixtures/ddlTranslatorResponses/" + fileName), responseType);
  }

  private void mockHttpClientService(
      Object mockResponse, int statusCode, boolean shouldThrowException) throws Exception {
    HttpResponse<String> httpResponse = mock(HttpResponse.class);
    when(httpResponse.body()).thenReturn(new ObjectMapper().writeValueAsString(mockResponse));
    when(httpResponse.statusCode()).thenReturn(statusCode);
    if (shouldThrowException) {
      when(httpClientService.sendPost(any(String.class), any(String.class)))
          .thenThrow(
              new DDLTranslationException(((ErrorResponse) mockResponse).getMessage(), null));
    } else {
      when(httpClientService.sendPost(any(String.class), any(String.class)))
          .thenReturn(httpResponse);
    }
  }
}
