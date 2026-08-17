package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;

/**
 * Literal nullability must survive Substrait → Calcite → Substrait conversion. Calcite types every
 * non-null literal NOT NULL, so a nullable Substrait literal travels as a nullability-only cast
 * that folds back into a nullable literal on the return trip.
 */
class LiteralNullabilityRoundtripTest extends PlanTestBase {

  final Rel table = sb.namedScan(List.of("example"), List.of("a"), List.of(N.I32));

  /** Every scalar literal kind, parameterized by nullability. */
  static List<Function<Boolean, Expression.Literal>> scalarLiterals() {
    return List.of(
        n -> ExpressionCreator.bool(n, true),
        n -> ExpressionCreator.i8(n, 12),
        n -> ExpressionCreator.i16(n, 1234),
        n -> ExpressionCreator.i32(n, 5),
        n -> ExpressionCreator.i64(n, 5L),
        n -> ExpressionCreator.fp32(n, 1.5f),
        n -> ExpressionCreator.fp64(n, 1.5),
        n -> ExpressionCreator.string(n, "x"),
        n -> ExpressionCreator.varChar(n, "x", 10),
        n -> ExpressionCreator.fixedChar(n, "x"),
        n -> ExpressionCreator.binary(n, new byte[] {1, 2}),
        n -> ExpressionCreator.fixedBinary(n, new byte[] {1, 2}),
        n -> ExpressionCreator.date(n, 19000),
        // precision 6 matches the microsecond normalization LiteralConverter applies on the way
        // back; precision_timestamp_tz is absent because it currently returns as a plain
        // precision_timestamp regardless of nullability
        n -> ExpressionCreator.precisionTime(n, 1_000_000L, 6),
        n -> ExpressionCreator.precisionTimestamp(n, 1_000_000L, 6),
        n -> ExpressionCreator.intervalYear(n, 1, 2),
        n -> ExpressionCreator.intervalDay(n, 1, 2, 3000, 6),
        n -> ExpressionCreator.decimal(n, BigDecimal.valueOf(123, 2), 10, 2));
  }

  private Expression roundtripExpression(Expression expression) {
    Project project = sb.project(input -> List.of(expression), Remap.of(List.of(1)), table);
    RelNode calcite = substraitToCalcite.convert(project);
    Rel back = SubstraitRelVisitor.convert(calcite, extensions);
    assertInstanceOf(Project.class, back);
    return ((Project) back).getExpressions().get(0);
  }

  private Expression.Literal roundtrip(Expression.Literal literal) {
    Expression expression = roundtripExpression(literal);
    assertInstanceOf(Expression.Literal.class, expression, "came back as: " + expression);
    return (Expression.Literal) expression;
  }

  @Test
  void nullableScalarsRoundTrip() {
    for (Function<Boolean, Expression.Literal> factory : scalarLiterals()) {
      Expression.Literal literal = factory.apply(true);
      assertEquals(literal, roundtrip(literal), () -> "for literal " + literal);
    }
  }

  @Test
  void nonNullableScalarsRoundTrip() {
    for (Function<Boolean, Expression.Literal> factory : scalarLiterals()) {
      Expression.Literal literal = factory.apply(false);
      assertEquals(literal, roundtrip(literal), () -> "for literal " + literal);
    }
  }

  @Test
  void typedNullRoundTrip() {
    Expression.Literal literal = ExpressionCreator.typedNull(N.I32);
    assertEquals(literal, roundtrip(literal));
  }

  /** A non-nullable literal keeps converting to a bare Calcite literal, with no cast wrapper. */
  @Test
  void nonNullableLiteralStaysBare() {
    Project project =
        sb.project(input -> List.of(ExpressionCreator.i32(false, 5)), Remap.of(List.of(1)), table);
    RelNode calcite = substraitToCalcite.convert(project);
    RexNode rex = ((LogicalProject) calcite).getProjects().get(0);
    assertEquals(SqlKind.LITERAL, rex.getKind());
  }

  /**
   * A nested list that is homogeneous on input — a nullable literal next to a nullable field
   * reference — must stay homogeneous through the round trip instead of failing the same-type
   * check.
   */
  @Test
  void nestedListWithNullableLiteralElement() {
    Expression nestedList =
        ExpressionCreator.nestedList(
            false, List.of(ExpressionCreator.i32(true, 5), sb.fieldReference(table, 0)));
    Expression back = roundtripExpression(nestedList);
    assertEquals(nestedList.getType(), back.getType());
  }
}
