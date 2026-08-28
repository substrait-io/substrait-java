package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import io.substrait.isthmus.expression.RexExpressionConverter;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SimpleExtendedExpressionsTest extends ExtendedExpressionTestBase {

  private static final String MARKER = "provider hook reached";

  private static Stream<Arguments> expressionTypeProvider() {
    return Stream.of(
        Arguments.of("2"), // I32LiteralExpression
        Arguments.of("L_ORDERKEY"), // FieldReferenceExpression
        Arguments.of("L_ORDERKEY > 10"), // ScalarFunctionExpressionFilter
        Arguments.of("L_ORDERKEY + 10"), // ScalarFunctionExpressionProjection
        Arguments.of("L_ORDERKEY IN (10, 20)"), // ScalarFunctionExpressionIn
        Arguments.of("L_ORDERKEY is not null"), // ScalarFunctionExpressionIsNotNull
        Arguments.of("L_ORDERKEY is null"), // ScalarFunctionExpressionIsNull
        // The rest are handled by CallConverters.defaults rather than by the scalar function
        // converter, and so reach this path only when it is assembled from the provider.
        Arguments.of("CAST(L_ORDERKEY AS VARCHAR)"), // CallConverters.CAST
        Arguments.of("CASE WHEN L_ORDERKEY > 10 THEN 1 ELSE 2 END"), // CallConverters.CASE
        Arguments.of("CURRENT_DATE"), // CallConverters.EXECUTION_CONTEXT_VARIABLE
        Arguments.of("ARRAY[1, 2]"), // SqlArrayValueConstructorCallConverter
        Arguments.of("MAP['a', 1]")); // SqlMapValueConstructorCallConverter
  }

  @ParameterizedTest
  @MethodSource("expressionTypeProvider")
  void testExtendedExpressionsRoundTrip(String sqlExpression)
      throws SqlParseException, IOException {
    assertProtoExtendedExpressionRoundtrip(sqlExpression);
  }

  @ParameterizedTest
  @MethodSource("expressionTypeProvider")
  void testExtendedExpressionsDuplicateColumnIdentifierRoundTrip(String sqlExpression) {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> assertProtoExtendedExpressionRoundtrip(sqlExpression, "tpch/schema_error.sql"));
    assertTrue(
        illegalArgumentException
            .getMessage()
            .startsWith("There is no support for duplicate column names"));
  }

  /**
   * The converter is taken from the provider rather than assembled here, so a provider that
   * overrides {@link ConverterProvider#getRexExpressionConverter} is honoured on this path as it
   * already is when converting a plan.
   */
  @Test
  void usesTheProvidersRexExpressionConverter() throws IOException {
    ConverterProvider provider =
        new ConverterProvider(ConverterProvider.builder()) {
          @Override
          public RexExpressionConverter getRexExpressionConverter(SubstraitRelVisitor srv) {
            throw new UnsupportedOperationException(MARKER);
          }
        };

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> new SqlExpressionToSubstrait(provider));
    assertEquals(MARKER, e.getMessage());
  }

  @Test
  void testExtendedExpressionsListExpressionRoundTrip() throws SqlParseException, IOException {
    String[] expressions = {
      "2",
      "L_ORDERKEY",
      "L_ORDERKEY > 10",
      "L_ORDERKEY + 10",
      "L_ORDERKEY IN (10, 20)",
      "L_ORDERKEY is not null",
      "L_ORDERKEY is null"
    };

    assertProtoExtendedExpressionRoundtrip(expressions);
  }

  /** The same for an outer reference, which is bound by a relation there is none of here. */
  @Test
  void anOuterReferenceIsRejectedRatherThanDereferencingTheMissingVisitor() {
    RexBuilder rexBuilder = new RexBuilder(SubstraitTypeSystem.TYPE_FACTORY);
    RexNode correlated =
        rexBuilder.makeCorrel(
            SubstraitTypeSystem.TYPE_FACTORY.builder().add("a", SqlTypeName.INTEGER).build(),
            new CorrelationId(0));
    RexFieldAccess fieldAccess = (RexFieldAccess) rexBuilder.makeFieldAccess(correlated, 0);

    UnsupportedOperationException rejected =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                ConverterProvider.DEFAULT
                    .getRexExpressionConverter(null)
                    .visitFieldAccess(fieldAccess));
    assertTrue(rejected.getMessage().contains("no relation visitor"), rejected.getMessage());
  }

  /**
   * The converter this path builds has no relation to visit, so a subquery cannot be converted on
   * it. What the path accepts today never produces one -- a constant scalar subquery folds before
   * the conversion sees it, and IN and EXISTS are rejected by Calcite's own validation -- so this
   * pins the converter rather than a SQL expression that reaches it.
   */
  @Test
  void aSubqueryIsRejectedRatherThanDereferencingTheMissingVisitor() {
    RexBuilder rexBuilder = new RexBuilder(SubstraitTypeSystem.TYPE_FACTORY);
    RelOptCluster cluster =
        RelOptCluster.create(new HepPlanner(HepProgram.builder().build()), rexBuilder);
    RelNode oneRow =
        LogicalValues.create(
            cluster,
            SubstraitTypeSystem.TYPE_FACTORY.builder().add("a", SqlTypeName.INTEGER).build(),
            ImmutableList.of(
                ImmutableList.of(
                    rexBuilder.makeExactLiteral(
                        java.math.BigDecimal.ONE,
                        SubstraitTypeSystem.TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)))));
    RexSubQuery subQuery = RexSubQuery.scalar(oneRow);

    UnsupportedOperationException rejected =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                ConverterProvider.DEFAULT.getRexExpressionConverter(null).visitSubQuery(subQuery));
    assertTrue(rejected.getMessage().contains("no relation visitor"), rejected.getMessage());
  }
}
