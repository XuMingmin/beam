/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.dsls.sql.rel;

import java.sql.Types;
import org.apache.beam.dsls.sql.BeamSqlCli;
import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.TestUtils;
import org.apache.beam.dsls.sql.mock.MockedBoundedTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for {@code BeamUnionRel}.
 */
public class BeamUnionRelTest {
  static BeamSqlEnv sqlEnv = new BeamSqlEnv();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void prepare() {
    sqlEnv.registerTable("ORDER_DETAILS",
        MockedBoundedTable.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        ).addRows(
            1L, 1, 1.0,
            2L, 2, 2.0
        )
    );
  }

  @Test
  public void testUnion() throws Exception {
    String sql = "SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + " UNION SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS ";

    PCollection<BeamSqlRecord> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        ).addRows(
            1L, 1, 1.0,
            2L, 2, 2.0
        ).getRows()
    );
    pipeline.run();
  }

  @Test
  public void testUnionAll() throws Exception {
    String sql = "SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS"
        + " UNION ALL "
        + " SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS";

    PCollection<BeamSqlRecord> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        ).addRows(
            1L, 1, 1.0,
            1L, 1, 1.0,
            2L, 2, 2.0,
            2L, 2, 2.0
        ).getRows()
    );
    pipeline.run();
  }
}
