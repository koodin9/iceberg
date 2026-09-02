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
package org.apache.iceberg.connect.data;

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Change type of a CDC record. Resolved from the record field named by {@code
 * iceberg.tables.cdc-field} and applied by {@link BaseDeltaWriter}.
 */
public enum Operation {
  INSERT,
  UPDATE,
  DELETE;

  /**
   * Parses an operation value case-insensitively. Accepts the codes produced by the Debezium and
   * DMS transforms ({@code I}, {@code U}, {@code D}), the raw Debezium codes ({@code c}, {@code r},
   * {@code u}, {@code d}) and the full operation names. Any other value is rejected so that a
   * misconfigured field is reported instead of being written as an insert.
   */
  public static Operation fromString(String value) {
    Preconditions.checkArgument(value != null, "Invalid CDC operation: null");
    switch (value.trim().toUpperCase(Locale.ROOT)) {
      case "I":
      case "C":
      case "R":
      case "INSERT":
        return INSERT;
      case "U":
      case "UPDATE":
        return UPDATE;
      case "D":
      case "DELETE":
        return DELETE;
      default:
        throw new IllegalArgumentException("Invalid CDC operation: " + value);
    }
  }
}
