package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
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

  /**
   * The shape the fold in {@link io.substrait.isthmus.expression.CallConverters} and the unwrap in
   * {@link SubstraitRelNodeConverter} both key on: a nullable literal is a cast of the literal to
   * its nullable type. The round trip passes either way, so this pins it directly.
   */
  @Test
  void nullableLiteralBecomesNullabilityCast() {
    Project project =
        sb.project(input -> List.of(ExpressionCreator.i32(true, 5)), Remap.of(List.of(1)), table);
    RelNode calcite = substraitToCalcite.convert(project);
    RexNode rex = ((LogicalProject) calcite).getProjects().get(0);
    assertEquals(SqlKind.CAST, rex.getKind());
    assertTrue(rex.getType().isNullable());
    assertEquals(SqlKind.LITERAL, ((RexCall) rex).getOperands().get(0).getKind());
  }

  /**
   * A cast doing more than nullability is not this representation, so the tuple unwrap leaves it
   * alone -- and a row whose values are not literals to Calcite is not a Values row at all, so it
   * takes the projection encoding with the cast still on it. The literal here is malformed input:
   * its value is longer than the length it declares, and nothing rejects it, here or in the POJO
   * that holds it. What this pins is only the cast, which is the part the conversion decides.
   */
  @Test
  void tupleUnwrapLeavesATruncatingCastAlone() {
    VirtualTableScan overlong =
        VirtualTableScan.builder()
            .initialSchema(NamedStruct.of(List.of("A"), R.struct(R.varChar(3))))
            .addRows(
                ExpressionCreator.nestedStruct(
                    false, ExpressionCreator.varChar(false, "abcdef", 3)))
            .build();

    assertEquals(
        "VirtualTable(rows=[[{ CAST('abcdef'):VARCHAR(3) NOT NULL }]])\n",
        RelOptUtil.toString(substraitToCalcite.convert(overlong)));
  }

  /**
   * The unwrap reads the Substrait expression, not just the Calcite node: a cast the plan carries
   * itself looks exactly like the one a nullable literal converts as, and dropping it would lose
   * its failure behavior and the {@link Expression.Cast} the round trip owes back. So a row holding
   * one is computed rather than tabulated.
   */
  @Test
  void aDeclaredNullabilityCastKeepsTheRowOffTheTuplePath() {
    VirtualTableScan castRow =
        VirtualTableScan.builder()
            .initialSchema(NamedStruct.of(List.of("A"), R.struct(N.I32)))
            .addRows(
                ExpressionCreator.nestedStruct(
                    false,
                    ExpressionCreator.cast(
                        N.I32,
                        ExpressionCreator.i32(false, 5),
                        Expression.FailureBehavior.THROW_EXCEPTION)))
            .build();

    assertEquals(
        "VirtualTable(rows=[[{ CAST(5):INTEGER }]])\n",
        RelOptUtil.toString(substraitToCalcite.convert(castRow)));
  }

  /**
   * The fold is nullability-only: a cast whose type equals the literal's exactly is not this
   * representation and stays an {@link Expression.Cast}. Built directly rather than round-tripped,
   * because Calcite drops an exact cast over a literal on the way out, so no plan reaches this
   * through the converter pair.
   */
  @Test
  void exactCastOverALiteralStaysACast() {
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelDataType i32 = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexNode exactCast =
        rexBuilder.makeAbstractCast(i32, rexBuilder.makeLiteral(5, i32, false), false);
    assertEquals(SqlKind.CAST, exactCast.getKind());

    Expression converted =
        CallConverters.CAST
            .apply(TypeConverter.DEFAULT)
            .apply((RexCall) exactCast, rex -> rex.accept(new RexExpressionConverter()));
    assertInstanceOf(Expression.Cast.class, converted, "converted to: " + converted);
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
