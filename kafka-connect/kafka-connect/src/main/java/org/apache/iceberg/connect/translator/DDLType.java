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

public enum DDLType {
  /** ALTER TABLE */
  ADD_COLUMN(true, true, false),
  MODIFY_COLUMN(true, true, false),
  DROP_COLUMN(true, true, false),
  RENAME_COLUMN(true, true, false),
  CHANGE_COLUMN(true, true, false),

  ALTER_COLUMN(true, true, false),

  INDEX(true, false, true),
  DROP_INDEX(true, false, true),

  ALGORITHM(true, false, false), // INPLACE, COPY, INSTANT..
  LOCK(true, false, false),

  TABLE_COMMENT(true, false, false),

  /* CONVERT char set */
  CONVERT(true, false, false),

  /* TRUNCATE */
  TRUNCATE(false, false, false),

  UNKNOWN(false, false, false);

  private final boolean isAlter;
  private final boolean isColumn;
  private final boolean isIndex;

  DDLType(boolean isAlter, boolean isColumn, boolean isIndex) {
    this.isAlter = isAlter;
    this.isColumn = isColumn;
    this.isIndex = isIndex;
  }

  public boolean isAlter() {
    return isAlter;
  }

  public boolean isColumn() {
    return isColumn;
  }

  public boolean isIndex() {
    return isIndex;
  }
}
