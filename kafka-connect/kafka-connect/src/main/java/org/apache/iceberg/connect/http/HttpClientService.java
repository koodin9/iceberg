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
package org.apache.iceberg.connect.http;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientService {

  private static final Logger LOG = LoggerFactory.getLogger(HttpClientService.class);

  private final HttpClient httpClient;
  private final Map<String, String> defaultHeaders;
  private final String baseUrl;
  private final ExecutorService executor;

  HttpClientService(String baseUrl, String apiToken, ExecutorService executor) {
    LOG.warn("🌐 HttpClientService initializing:");
    LOG.warn("🌐 baseUrl: {}", baseUrl);
    LOG.warn("🌐 token: {}", apiToken);

    this.baseUrl = baseUrl;
    this.executor = executor;
    this.httpClient = initializeHttpClient();
    this.defaultHeaders = initializeHeaders(apiToken);
  }

  private HttpClient initializeHttpClient() {
    return HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(30))
        .executor(executor) // 커스텀 스레드 풀 사용
        .build();
  }

  private Map<String, String> initializeHeaders(String apiToken) {
    Map<String, String> headers = Maps.newHashMap();
    headers.put("Accept", "*/*");
    headers.put("Content-Type", "application/json");
    if (apiToken != null) {
      headers.put("Token", apiToken);
    }
    return Collections.unmodifiableMap(headers);
  }

  public HttpResponse<String> sendGet(String url) throws HttpException {
    return sendGetAsync(url).join();
  }

  public HttpResponse<String> sendPost(String url, String jsonPayload) throws HttpException {
    return sendPostAsync(url, jsonPayload).join();
  }

  public HttpResponse<String> sendDelete(String url) throws HttpException {
    return sendDeleteAsync(url).join();
  }

  public HttpResponse<String> sendPatch(String url, String jsonPayload) throws HttpException {
    return sendPatchAsync(url, jsonPayload).join();
  }

  // 비동기 메서드들

  public CompletableFuture<HttpResponse<String>> sendGetAsync(String url) {
    try {
      LOG.info("🌐 Sending GET request: {}", url);
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(baseUrl + url))
              .GET()
              .timeout(Duration.ofSeconds(30))
              .headers(createHeadersArray())
              .build();

      return httpClient
          .sendAsync(request, HttpResponse.BodyHandlers.ofString())
          .thenApply(this::handleResponseStatus)
          .exceptionally(this::handleException);

    } catch (URISyntaxException e) {
      LOG.error("🌐 Invalid URI for GET request: {}", e.getMessage(), e);
      return CompletableFuture.failedFuture(
          new HttpException(HttpException.CLIENT_ERROR_STATUS, "Invalid URI", e));
    }
  }

  public CompletableFuture<HttpResponse<String>> sendPostAsync(String url, String jsonPayload) {
    try {
      LOG.info("🌐 Sending POST request: {}, {}", url, jsonPayload);
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(baseUrl + url))
              .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
              .timeout(Duration.ofSeconds(30))
              .headers(createHeadersArray())
              .build();

      return httpClient
          .sendAsync(request, HttpResponse.BodyHandlers.ofString())
          .thenApply(this::handleResponseStatus)
          .exceptionally(this::handleException);

    } catch (URISyntaxException e) {
      LOG.error("🌐 Invalid URI for POST request: {}", e.getMessage(), e);
      return CompletableFuture.failedFuture(
          new HttpException(HttpException.CLIENT_ERROR_STATUS, "Invalid URI", e));
    }
  }

  public CompletableFuture<HttpResponse<String>> sendDeleteAsync(String url) {
    try {
      LOG.info("🌐 Sending DELETE request: {}", url);
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(baseUrl + url))
              .DELETE()
              .timeout(Duration.ofSeconds(30))
              .headers(createHeadersArray())
              .build();

      return httpClient
          .sendAsync(request, HttpResponse.BodyHandlers.ofString())
          .thenApply(this::handleResponseStatus)
          .exceptionally(this::handleException);

    } catch (URISyntaxException e) {
      LOG.error("🌐 Invalid URI for DELETE request: {}", e.getMessage(), e);
      return CompletableFuture.failedFuture(
          new HttpException(HttpException.CLIENT_ERROR_STATUS, "Invalid URI", e));
    }
  }

  public CompletableFuture<HttpResponse<String>> sendPatchAsync(String url, String jsonPayload) {
    try {
      LOG.info("🌐 Sending PATCH request: {}, {}", url, jsonPayload);
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(new URI(baseUrl + url))
              .method("PATCH", HttpRequest.BodyPublishers.ofString(jsonPayload))
              .timeout(Duration.ofSeconds(30))
              .headers(createHeadersArray())
              .build();

      return httpClient
          .sendAsync(request, HttpResponse.BodyHandlers.ofString())
          .thenApply(this::handleResponseStatus)
          .exceptionally(this::handleException);

    } catch (URISyntaxException e) {
      LOG.error("🌐 Invalid URI for PATCH request: {}", e.getMessage(), e);
      return CompletableFuture.failedFuture(
          new HttpException(HttpException.CLIENT_ERROR_STATUS, "Invalid URI", e));
    }
  }

  private String[] createHeadersArray() {
    return defaultHeaders.entrySet().stream()
        .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
        .toArray(String[]::new);
  }

  private HttpResponse<String> handleResponseStatus(HttpResponse<String> response) {
    if (response.statusCode() >= HttpException.CLIENT_ERROR_STATUS) {
      throw new RuntimeException(
          new HttpException(
              response.statusCode(),
              "HTTP request failed with status code "
                  + response.statusCode()
                  + ", url:"
                  + response.uri()));
    }
    return response;
  }

  private HttpResponse<String> handleException(Throwable throwable) {
    LOG.error("🌐 HTTP request failed: {}", throwable.getMessage(), throwable);

    if (throwable.getCause() instanceof HttpException) {
      throw new RuntimeException(throwable.getCause());
    }

    throw new RuntimeException(
        new HttpException(HttpException.SERVER_ERROR_STATUS, "HTTP request failed", throwable));
  }
}
