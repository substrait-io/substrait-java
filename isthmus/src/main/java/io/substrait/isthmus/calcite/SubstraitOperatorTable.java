package io.substrait.isthmus.calcite;

import io.substrait.isthmus.AggregateFunctions;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.jspecify.annotations.Nullable;

/**
 * SQL operator table that prioritizes Substrait-specific operator variants where available, falling
 * back to extended library operators and the standard Calcite operator table.
 *
 * <p>Overrides lookups to return Substrait variants first (e.g., {@link AggregateFunctions#MAX}),
 * ensuring deterministic resolution when multiple implementations exist.
 *
 * @see SqlOperatorTable
 */
public class SubstraitOperatorTable implements SqlOperatorTable {

  /** Singleton instance of the Substrait operator table. */
  public static SubstraitOperatorTable INSTANCE = new SubstraitOperatorTable();

  private static final SqlOperatorTable SUBSTRAIT_OPERATOR_TABLE =
      SqlOperatorTables.of(
          List.of(
              AggregateFunctions.MAX,
              AggregateFunctions.MIN,
              AggregateFunctions.AVG,
              AggregateFunctions.SUM,
              AggregateFunctions.SUM0));

  // SQL Kinds for which Substrait specific operators are provided
  private static final Set<SqlKind> OVERRIDE_KINDS =
      EnumSet.copyOf(
          SUBSTRAIT_OPERATOR_TABLE.getOperatorList().stream()
              .map(SqlOperator::getKind)
              .collect(Collectors.toList()));

  // Utilisation of extended library operators available from calcite 1.35+, i.e hyperbolic
  // functions
  private static final SqlOperatorTable LIBRARY_OPERATOR_TABLE =
      SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
          EnumSet.of(
              SqlLibrary.HIVE,
              SqlLibrary.SPARK,
              SqlLibrary.ALL,
              SqlLibrary.BIG_QUERY,
              SqlLibrary.SNOWFLAKE,
              SqlLibrary.STANDARD));

  private static final SqlOperatorTable STANDARD_OPERATOR_TABLE = SqlStdOperatorTable.instance();

  private static final List<SqlOperator> OPERATOR_LIST =
      Stream.concat(
              SUBSTRAIT_OPERATOR_TABLE.getOperatorList().stream(),
              Stream.concat(
                  LIBRARY_OPERATOR_TABLE.getOperatorList().stream(),
                  // filter out the kinds that have been overriden from the standard operator table
                  STANDARD_OPERATOR_TABLE.getOperatorList().stream()
                      .filter(op -> !OVERRIDE_KINDS.contains(op.kind))))
          .collect(Collectors.toUnmodifiableList());

  /** Private constructor. */
  private SubstraitOperatorTable() {}

  /**
   * Looks up operators by name and syntax, preferring Substrait variants first, then library
   * operators, and finally the standard operator table.
   *
   * <p>If a Substrait operator match is found, it is returned immediately to avoid ambiguous
   * resolution when multiple matches exist.
   *
   * @param opName The operator name as a {@link SqlIdentifier}.
   * @param category Optional {@link SqlFunctionCategory} to narrow the lookup; may be {@code null}.
   * @param syntax The {@link SqlSyntax} (e.g., FUNCTION, BINARY, SPECIAL).
   * @param operatorList Output list to which matching {@link SqlOperator}s are added.
   * @param nameMatcher The {@link SqlNameMatcher} used to match names.
   */
  @Override
  public void lookupOperatorOverloads(
      SqlIdentifier opName,
      @Nullable SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList,
      SqlNameMatcher nameMatcher) {
    SUBSTRAIT_OPERATOR_TABLE.lookupOperatorOverloads(
        opName, category, syntax, operatorList, nameMatcher);
    if (!operatorList.isEmpty()) {
      // If a match for a Substrait operator is found, return it immediately.
      // Without this, Calcite will find multiple matches for the same operator.
      // It then fails to resolve a specific operator as it can't pick between them
      return;
    }

    LIBRARY_OPERATOR_TABLE.lookupOperatorOverloads(
        opName, category, syntax, operatorList, nameMatcher);
    if (!operatorList.isEmpty()) {
      return;
    }

    STANDARD_OPERATOR_TABLE.lookupOperatorOverloads(
        opName, category, syntax, operatorList, nameMatcher);
  }

  /**
   * Returns the combined operator list, including Substrait operators, extended library operators,
   * and standard operators (excluding kinds overridden by Substrait).
   *
   * @return Immutable list of all available {@link SqlOperator}s.
   */
  @Override
  public List<SqlOperator> getOperatorList() {
    return OPERATOR_LIST;
  }
}
