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
package org.apache.iceberg.connect.cmdb.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum PartitionStatus {
  IN_PROGRESS("in_progress"),
  PENDING("pending"),
  DONE("done");

  private final String value;

  PartitionStatus(String value) {
    this.value = value;
  }

  public boolean isInProgress() {
    return this == IN_PROGRESS;
  }

  public boolean isPending() {
    return this == PENDING;
  }

  public String getValue() {
    return value;
  }

  @JsonValue
  @Override
  public String toString() {
    return value;
  }

  @JsonCreator
  public static PartitionStatus fromValue(String value) {
    for (PartitionStatus status : PartitionStatus.values()) {
      if (status.value.equalsIgnoreCase(value)) {
        return status;
      }
    }
    throw new IllegalArgumentException("Unknown enum value: " + value);
  }
}
