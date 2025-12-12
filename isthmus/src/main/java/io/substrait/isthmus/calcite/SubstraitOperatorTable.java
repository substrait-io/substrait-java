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
 * Overrides SQL operator lookups to return Substrait specific functions variants (e.g. {@link
 * AggregateFunctions#MAX}} when they are available.
 */
public class SubstraitOperatorTable implements SqlOperatorTable {

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
          EnumSet.of(SqlLibrary.HIVE, SqlLibrary.SPARK, SqlLibrary.ALL));

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

  private SubstraitOperatorTable() {}

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

  @Override
  public List<SqlOperator> getOperatorList() {
    return OPERATOR_LIST;
  }
}
