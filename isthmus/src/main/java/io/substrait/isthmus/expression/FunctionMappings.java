package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class FunctionMappings {
  // Static list of signature mapping between Calcite SQL operators and Substrait base function
  // names.
  public static final ImmutableList<Sig> SCALAR_SIGS;
  public static final ImmutableList<Sig> AGGREGATE_SIGS;
  public static final ImmutableList<Sig> WINDOW_SIGS;
  public static final Map<SqlOperator, TypeBasedResolver> OPERATOR_RESOLVER;

  static {
    SCALAR_SIGS =
        ImmutableList.<Sig>builder()
            .add(
                s(SqlStdOperatorTable.PLUS, "add"),
                s(SqlStdOperatorTable.MINUS, "subtract"),
                s(SqlStdOperatorTable.UNARY_MINUS, "negate"),
                s(SqlStdOperatorTable.MULTIPLY, "multiply"),
                s(SqlStdOperatorTable.DIVIDE, "divide"),
                s(SqlStdOperatorTable.ABS, "abs"),
                s(SqlStdOperatorTable.MOD, "modulus"),
                s(SqlStdOperatorTable.POWER, "power"),
                s(SqlStdOperatorTable.EXP, "exp"),
                s(SqlStdOperatorTable.SIN, "sin"),
                s(SqlStdOperatorTable.COS, "cos"),
                s(SqlStdOperatorTable.TAN, "tan"),
                s(SqlStdOperatorTable.ASIN, "asin"),
                s(SqlStdOperatorTable.ACOS, "acos"),
                s(SqlStdOperatorTable.ATAN, "atan"),
                s(SqlStdOperatorTable.ATAN2, "atan2"),
                s(SqlStdOperatorTable.SIGN, "sign"),
                s(SqlStdOperatorTable.LOG10, "log10"),
                s(SqlStdOperatorTable.LN, "ln"),
                s(SqlStdOperatorTable.AND),
                s(SqlStdOperatorTable.OR),
                s(SqlStdOperatorTable.NOT),
                s(SqlStdOperatorTable.LESS_THAN, "lt"),
                s(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "lte"),
                s(SqlStdOperatorTable.GREATER_THAN, "gt"),
                s(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "gte"),
                s(SqlStdOperatorTable.EQUALS, "equal"),
                s(SqlStdOperatorTable.BIT_XOR, "xor"),
                s(SqlStdOperatorTable.IS_NULL, "is_null"),
                s(SqlStdOperatorTable.IS_NOT_NULL, "is_not_null"),
                s(SqlStdOperatorTable.NOT_EQUALS, "not_equal"),
                s(SqlStdOperatorTable.MINUS_DATE, "subtract"),
                s(SqlStdOperatorTable.DATETIME_PLUS, "add"),
                s(SqlStdOperatorTable.EXTRACT, "extract"),
                s(SqlStdOperatorTable.CEIL, "ceil"),
                s(SqlStdOperatorTable.FLOOR, "floor"),
                s(SqlStdOperatorTable.ROUND, "round"),
                s(SqlStdOperatorTable.LIKE),
                s(SqlStdOperatorTable.SUBSTRING, "substring"),
                s(SqlStdOperatorTable.CONCAT, "concat"),
                s(SqlStdOperatorTable.LOWER, "lower"),
                s(SqlStdOperatorTable.UPPER, "upper"),
                s(SqlStdOperatorTable.BETWEEN),
                s(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, "is_not_distinct_from"),
                s(SqlStdOperatorTable.COALESCE, "coalesce"))
            .build();

    AGGREGATE_SIGS =
        ImmutableList.<Sig>builder()
            .add(
                s(SqlStdOperatorTable.MIN, "min"),
                s(SqlStdOperatorTable.MAX, "max"),
                s(SqlStdOperatorTable.SUM, "sum"),
                s(SqlStdOperatorTable.SUM0, "sum0"),
                s(SqlStdOperatorTable.COUNT, "count"),
                s(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, "approx_count_distinct"),
                s(SqlStdOperatorTable.AVG, "avg"))
            .build();

    WINDOW_SIGS =
        ImmutableList.<Sig>builder()
            .add(
                s(SqlStdOperatorTable.ROW_NUMBER, "row_number"),
                s(SqlStdOperatorTable.LAG, "lag"),
                s(SqlStdOperatorTable.LEAD, "lead"),
                s(SqlStdOperatorTable.RANK, "rank"),
                s(SqlStdOperatorTable.DENSE_RANK, "dense_rank"),
                s(SqlStdOperatorTable.PERCENT_RANK, "percent_rank"),
                s(SqlStdOperatorTable.CUME_DIST, "cume_dist"),
                s(SqlStdOperatorTable.NTILE, "ntile"),
                s(SqlStdOperatorTable.FIRST_VALUE, "first_value"),
                s(SqlStdOperatorTable.LAST_VALUE, "last_value"),
                s(SqlStdOperatorTable.NTH_VALUE, "nth_value"))
            // Aggregate Functions can be used in Windows
            .addAll(AGGREGATE_SIGS)
            .build();

    // contains return-type based resolver for both scalar and aggregator operator
    OPERATOR_RESOLVER =
        Map.of(
            SqlStdOperatorTable.PLUS,
                resolver(
                    SqlStdOperatorTable.PLUS,
                    Set.of("i8", "i16", "i32", "i64", "fp32", "fp64", "dec")),
            SqlStdOperatorTable.DATETIME_PLUS,
                resolver(SqlStdOperatorTable.PLUS, Set.of("date", "time", "timestamp")),
            SqlStdOperatorTable.MINUS,
                resolver(
                    SqlStdOperatorTable.MINUS,
                    Set.of("i8", "i16", "i32", "i64", "fp32", "fp64", "dec")),
            SqlStdOperatorTable.MINUS_DATE,
                resolver(
                    SqlStdOperatorTable.MINUS_DATE, Set.of("date", "timestamp_tz", "timestamp")));
  }

  public static void main(String[] args) {
    SCALAR_SIGS.forEach(System.out::println);
  }

  public static Sig s(SqlOperator operator, String substraitName) {
    return new Sig(operator, substraitName.toLowerCase(Locale.ROOT));
  }

  public static Sig s(SqlOperator operator) {
    return s(operator, operator.getName().toLowerCase(Locale.ROOT));
  }

  public static class Sig {
    public final SqlOperator operator;
    public final String name;

    public Sig(final SqlOperator operator, final String name) {
      this.operator = operator;
      this.name = name;
    }

    public String name() {
      return name;
    }

    public SqlOperator operator() {
      return operator;
    }
  }

  public static TypeBasedResolver resolver(SqlOperator operator, Set<String> outTypes) {
    return new TypeBasedResolver(operator, outTypes);
  }

  public static class TypeBasedResolver {

    public final SqlOperator operator;
    public final Set<String> types;

    public TypeBasedResolver(final SqlOperator operator, final Set<String> types) {
      this.operator = operator;
      this.types = types;
    }

    public SqlOperator operator() {
      return operator;
    }

    public Set<String> types() {
      return types;
    }
  }
}
