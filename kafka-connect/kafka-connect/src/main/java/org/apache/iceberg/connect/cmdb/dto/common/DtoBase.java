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
package org.apache.iceberg.connect.cmdb.dto.common;

import java.lang.reflect.Field;
import java.util.StringJoiner;

public abstract class DtoBase {

  public DtoBase() {}

  @Override
  public final String toString() {
    StringJoiner joiner = new StringJoiner(", ", getClass().getSimpleName() + "[", "]");

    Field[] fields = getClass().getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true); // private 필드 접근 허용
      try {
        Object value = field.get(this);
        joiner.add(field.getName() + "=" + value);
      } catch (IllegalAccessException e) {
        joiner.add(field.getName() + "=<inaccessible>");
      }
    }

    return joiner.toString();
  }
}
