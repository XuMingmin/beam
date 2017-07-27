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

package org.apache.beam.dsls.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamRow;
import org.apache.beam.dsls.sql.schema.BeamRowType;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Test utilities.
 */
public class TestUtils {
  /**
   * A {@code DoFn} to convert a {@code BeamSqlRow} to a comparable {@code String}.
   */
  public static class BeamSqlRow2StringDoFn extends DoFn<BeamRow, String> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element().valueInString());
    }
  }

  /**
   * Convert list of {@code BeamSqlRow} to list of {@code String}.
   */
  public static List<String> beamSqlRows2Strings(List<BeamRow> rows) {
    List<String> strs = new ArrayList<>();
    for (BeamRow row : rows) {
      strs.add(row.valueInString());
    }

    return strs;
  }

  /**
   * Convenient way to build a list of {@code BeamSqlRow}s.
   *
   * <p>You can use it like this:
   *
   * <pre>{@code
   * TestUtils.RowsBuilder.of(
   *   Types.INTEGER, "order_id",
   *   Types.INTEGER, "sum_site_id",
   *   Types.VARCHAR, "buyer"
   * ).addRows(
   *   1, 3, "james",
   *   2, 5, "bond"
   *   ).getStringRows()
   * }</pre>
   * {@code}
   */
  public static class RowsBuilder {
    private BeamRowType type;
    private List<BeamRow> rows = new ArrayList<>();

    /**
     * Create a RowsBuilder with the specified row type info.
     *
     * <p>For example:
     * <pre>{@code
     * TestUtils.RowsBuilder.of(
     *   Types.INTEGER, "order_id",
     *   Types.INTEGER, "sum_site_id",
     *   Types.VARCHAR, "buyer"
     * )}</pre>
     *
     * @args pairs of column type and column names.
     */
    public static RowsBuilder of(final Object... args) {
      BeamRowType beamSQLRowType = buildBeamSqlRowType(args);
      RowsBuilder builder = new RowsBuilder();
      builder.type = beamSQLRowType;

      return builder;
    }

    /**
     * Create a RowsBuilder with the specified row type info.
     *
     * <p>For example:
     * <pre>{@code
     * TestUtils.RowsBuilder.of(
     *   beamSqlRowType
     * )}</pre>
     * @beamSQLRowType the record type.
     */
    public static RowsBuilder of(final BeamRowType beamSQLRowType) {
      RowsBuilder builder = new RowsBuilder();
      builder.type = beamSQLRowType;

      return builder;
    }

    /**
     * Add rows to the builder.
     *
     * <p>Note: check the class javadoc for for detailed example.
     */
    public RowsBuilder addRows(final Object... args) {
      this.rows.addAll(buildRows(type, Arrays.asList(args)));
      return this;
    }

    /**
     * Add rows to the builder.
     *
     * <p>Note: check the class javadoc for for detailed example.
     */
    public RowsBuilder addRows(final List args) {
      this.rows.addAll(buildRows(type, args));
      return this;
    }

    public List<BeamRow> getRows() {
      return rows;
    }

    public List<String> getStringRows() {
      return beamSqlRows2Strings(rows);
    }
  }

  /**
   * Convenient way to build a {@code BeamSqlRowType}.
   *
   * <p>e.g.
   *
   * <pre>{@code
   *   buildBeamSqlRowType(
   *       Types.BIGINT, "order_id",
   *       Types.INTEGER, "site_id",
   *       Types.DOUBLE, "price",
   *       Types.TIMESTAMP, "order_time"
   *   )
   * }</pre>
   */
  public static BeamRowType buildBeamSqlRowType(Object... args) {
    List<Integer> types = new ArrayList<>();
    List<String> names = new ArrayList<>();

    for (int i = 0; i < args.length - 1; i += 2) {
      types.add((int) args[i]);
      names.add((String) args[i + 1]);
    }

    return BeamRowType.create(names, types);
  }

  /**
   * Convenient way to build a {@code BeamSqlRow}s.
   *
   * <p>e.g.
   *
   * <pre>{@code
   *   buildRows(
   *       rowType,
   *       1, 1, 1, // the first row
   *       2, 2, 2, // the second row
   *       ...
   *   )
   * }</pre>
   */
  public static List<BeamRow> buildRows(BeamRowType type, List args) {
    List<BeamRow> rows = new ArrayList<>();
    int fieldCount = type.size();

    for (int i = 0; i < args.size(); i += fieldCount) {
      BeamRow row = new BeamRow(type);
      for (int j = 0; j < fieldCount; j++) {
        row.addField(j, args.get(i + j));
      }
      rows.add(row);
    }
    return rows;
  }
}
