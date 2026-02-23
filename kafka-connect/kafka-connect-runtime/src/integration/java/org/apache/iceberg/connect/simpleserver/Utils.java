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

import jakarta.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;

public class Utils {

  private Utils() {}

  // JSON Body 읽기 헬퍼 메서드
  public static String readJsonBody(HttpServletRequest req) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader reader = req.getReader()) {
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
    }
    return sb.toString();
  }

  public static Long getIdFromPath(HttpServletRequest req) {
    String pathInfo = req.getPathInfo();
    if (pathInfo == null || pathInfo.equals("/")) {
      return null; // ID가 없는 경우
    }

    String idString = pathInfo.substring(1); // "/3" -> "3"
    try {
      return Long.parseLong(idString);
    } catch (NumberFormatException e) {
      return null; // 잘못된 ID 형식
    }
  }
}
