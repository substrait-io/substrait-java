package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.isthmus.AggregateFunctions;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Defines static mappings between Calcite {@link SqlOperator} signatures and Substrait base
 * function names, including scalar, aggregate, and window function signatures.
 *
 * <p>Also provides type-based resolvers to disambiguate operators by output type.
 */
public class FunctionMappings {

  /** Scalar operator signatures mapped to Substrait function names. */
  public static final ImmutableList<Sig> SCALAR_SIGS =
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
              s(SqlStdOperatorTable.REPLACE, "replace"),
              s(SqlStdOperatorTable.SUBSTRING, "substring"),
              s(SqlStdOperatorTable.CONCAT, "concat"),
              s(SqlStdOperatorTable.CHAR_LENGTH, "char_length"),
              s(SqlStdOperatorTable.LOWER, "lower"),
              s(SqlStdOperatorTable.UPPER, "upper"),
              s(SqlStdOperatorTable.BETWEEN),
              s(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, "is_not_distinct_from"),
              s(SqlStdOperatorTable.COALESCE, "coalesce"),
              s(SqlStdOperatorTable.TRIM, "trim"),
              s(SqlStdOperatorTable.TRIM, "ltrim"),
              s(SqlStdOperatorTable.TRIM, "rtrim"),
              s(SqlStdOperatorTable.SQRT, "sqrt"),
              s(SqlLibraryOperators.SINH, "sinh"),
              s(SqlLibraryOperators.TANH, "tanh"),
              s(SqlLibraryOperators.COSH, "cosh"),
              s(SqlLibraryOperators.ASINH, "asinh"),
              s(SqlLibraryOperators.ATANH, "atanh"),
              s(SqlLibraryOperators.ACOSH, "acosh"),
              s(SqlStdOperatorTable.BITNOT, "bitwise_not"),
              s(SqlStdOperatorTable.BITOR, "bitwise_or"),
              s(SqlStdOperatorTable.BITAND, "bitwise_and"),
              s(SqlStdOperatorTable.BITXOR, "bitwise_xor"),
              s(SqlStdOperatorTable.RADIANS, "radians"),
              s(SqlStdOperatorTable.DEGREES, "degrees"),
              s(SqlLibraryOperators.FACTORIAL, "factorial"),
              s(SqlStdOperatorTable.IS_TRUE, "is_true"),
              s(SqlStdOperatorTable.IS_FALSE, "is_false"),
              s(SqlStdOperatorTable.IS_NOT_TRUE, "is_not_true"),
              s(SqlStdOperatorTable.IS_NOT_FALSE, "is_not_false"),
              s(SqlStdOperatorTable.IS_DISTINCT_FROM, "is_distinct_from"),
              s(SqlLibraryOperators.LOG2, "log2"),
              s(SqlLibraryOperators.LEAST, "least"),
              s(SqlLibraryOperators.GREATEST, "greatest"),
              s(SqlStdOperatorTable.BIT_LEFT_SHIFT, "shift_left"),
              s(SqlStdOperatorTable.LEFTSHIFT, "shift_left"),
              s(SqlLibraryOperators.STARTS_WITH, "starts_with"),
              s(SqlLibraryOperators.ENDS_WITH, "ends_with"),
              s(SqlLibraryOperators.CONTAINS_SUBSTR, "contains"),
              s(SqlStdOperatorTable.POSITION, "strpos"),
              s(SqlLibraryOperators.LEFT, "left"),
              s(SqlLibraryOperators.RIGHT, "right"),
              s(SqlLibraryOperators.LPAD, "lpad"),
              s(SqlLibraryOperators.RPAD, "rpad"))
          .build();

  /** Aggregate operator signatures mapped to Substrait function names. */
  public static final ImmutableList<Sig> AGGREGATE_SIGS =
      ImmutableList.<Sig>builder()
          .add(
              s(AggregateFunctions.MIN, "min"),
              s(AggregateFunctions.MAX, "max"),
              s(AggregateFunctions.SUM, "sum"),
              s(AggregateFunctions.SUM0, "sum0"),
              s(SqlStdOperatorTable.COUNT, "count"),
              s(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, "approx_count_distinct"),
              s(AggregateFunctions.AVG, "avg"))
          .build();

  /** Window function signatures (including supported aggregates) mapped to Substrait names. */
  public static final ImmutableList<Sig> WINDOW_SIGS =
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

  /** Type-based resolvers to disambiguate Calcite operators by expected output type. */
  public static final Map<SqlOperator, TypeBasedResolver> OPERATOR_RESOLVER =
      Map.of(
          SqlStdOperatorTable.PLUS,
          resolver(
              SqlStdOperatorTable.PLUS, Set.of("i8", "i16", "i32", "i64", "fp32", "fp64", "dec")),
          SqlStdOperatorTable.DATETIME_PLUS,
          resolver(SqlStdOperatorTable.PLUS, Set.of("date", "time", "timestamp")),
          SqlStdOperatorTable.MINUS,
          resolver(
              SqlStdOperatorTable.MINUS, Set.of("i8", "i16", "i32", "i64", "fp32", "fp64", "dec")),
          SqlStdOperatorTable.MINUS_DATE,
          resolver(SqlStdOperatorTable.MINUS_DATE, Set.of("date", "timestamp_tz", "timestamp")),
          SqlStdOperatorTable.BIT_LEFT_SHIFT,
          resolver(SqlStdOperatorTable.BIT_LEFT_SHIFT, Set.of("i8", "i16", "i32", "i64")));

  /**
   * Prints all scalar signatures (for quick inspection).
   *
   * @param args CLI arguments (unused)
   */
  public static void main(String[] args) {
    SCALAR_SIGS.forEach(System.out::println);
  }

  /**
   * Creates a signature mapping entry.
   *
   * @param operator the Calcite operator
   * @param substraitName the Substrait canonical function name
   * @return a {@link Sig} instance
   */
  public static Sig s(SqlOperator operator, String substraitName) {
    return new Sig(operator, substraitName.toLowerCase(Locale.ROOT));
  }

  /**
   * Creates a signature mapping entry using the operator's own (lowercased) name.
   *
   * @param operator the Calcite operator
   * @return a {@link Sig} instance
   */
  public static Sig s(SqlOperator operator) {
    return s(operator, operator.getName().toLowerCase(Locale.ROOT));
  }

  /** Simple signature tuple of operator to Substrait name. */
  public static class Sig {

    /** SqlOperator. */
    public final SqlOperator operator;

    /** Name. */
    public final String name;

    /**
     * Constructs a signature entry.
     *
     * @param operator the Calcite operator
     * @param name the Substrait function name
     */
    public Sig(final SqlOperator operator, final String name) {
      this.operator = operator;
      this.name = name;
    }

    /**
     * Returns the Substrait function name.
     *
     * @return the Substrait name
     */
    public String name() {
      return name;
    }

    /**
     * Returns the Calcite operator.
     *
     * @return the operator
     */
    public SqlOperator operator() {
      return operator;
    }
  }

  /**
   * Creates a type-based resolver for an operator.
   *
   * @param operator the Calcite operator
   * @param outTypes the set of allowed output type strings (Substrait)
   * @return a {@link TypeBasedResolver}
   */
  public static TypeBasedResolver resolver(SqlOperator operator, Set<String> outTypes) {
    return new TypeBasedResolver(operator, outTypes);
  }

  /** Disambiguates operators based on expected output type strings. */
  public static class TypeBasedResolver {

    /** SqlOperator. */
    public final SqlOperator operator;

    /** Types. */
    public final Set<String> types;

    /**
     * Constructs a resolver.
     *
     * @param operator the Calcite operator
     * @param types allowed output type strings
     */
    public TypeBasedResolver(final SqlOperator operator, final Set<String> types) {
      this.operator = operator;
      this.types = types;
    }

    /**
     * Returns the operator this resolver applies to.
     *
     * @return the operator
     */
    public SqlOperator operator() {
      return operator;
    }

    /**
     * Returns the allowed output type strings.
     *
     * @return set of type strings
     */
    public Set<String> types() {
      return types;
    }
  }
}
