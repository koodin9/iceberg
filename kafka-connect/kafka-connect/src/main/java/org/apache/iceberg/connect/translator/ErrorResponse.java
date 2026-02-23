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

public class ErrorResponse {

  private String timestamp;
  private int status;
  private String error;
  private String code;
  private String message;

  public ErrorResponse() {}

  public ErrorResponse(String timestamp, int status, String error, String code, String message) {
    this.timestamp = timestamp;
    this.status = status;
    this.error = error;
    this.code = code;
    this.message = message;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public int getStatus() {
    return status;
  }

  public String getError() {
    return error;
  }

  public String getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }
}
