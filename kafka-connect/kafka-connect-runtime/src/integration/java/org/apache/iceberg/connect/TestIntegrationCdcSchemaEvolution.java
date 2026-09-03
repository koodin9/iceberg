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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * CDC while the table schema evolves: a new field arrives in the middle of a batch, the writer is
 * replaced with one for the new schema, and the deletes of the batch are still converted.
 */
public class TestIntegrationCdcSchemaEvolution extends CdcIntegrationTestBase {

  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, "cdc_evolve");

  @Test
  public void testNewColumnDuringCdc() {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            TestEvent.TEST_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    startCdcConnector(createConfig(false));

    sendEvent(event(1, "a", "v1", "I"));
    sendEvent(event(2, "a", "v1", "I"));
    flushEvents();
    awaitRows(TABLE_IDENTIFIER, expected("1|v1", "2|v1"), "id", "payload");

    // the update carries a field the table does not have yet
    Map<String, Object> update = event(1, "a", "v2", "U");
    update.put("extra", "x");
    sendEvent(update);
    Map<String, Object> insert = event(3, "a", "v1", "I");
    insert.put("extra", "y");
    sendEvent(insert);
    sendEvent(event(2, "a", "v1", "D"));
    flushEvents();

    awaitRows(TABLE_IDENTIFIER, expected("1|v2|x", "3|v1|y"), "id", "payload", "extra");

    Schema schema = catalog().loadTable(TABLE_IDENTIFIER).schema();
    assertThat(schema.findField("extra")).isNotNull();
    assertThat(schema.findField("extra").type()).isEqualTo(Types.StringType.get());
    // the identifier is kept through the evolution
    assertThat(schema.identifierFieldIds()).containsExactly(1);

    assertOnlyDeletionVectors(TABLE_IDENTIFIER);
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return cdcConfig(TABLE_IDENTIFIER).config("iceberg.tables.evolve-schema-enabled", true);
  }

  @Override
  void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }
}
