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
package org.apache.iceberg.connect.common;

public enum AlertTarget {
  MANAGER,
  USER,
  ALL;

  /** manager 알람에 쓰이는 watchcenter ID는 connect-admin 에 하드코딩 되어있음. prod: 22356 dev: 23136 */
  public String[] toArray() {
    return switch (this) {
      case MANAGER -> new String[] {"manager"};
      case USER -> new String[] {"user"};
      case ALL -> new String[] {"manager", "user"};
    };
  }
}
