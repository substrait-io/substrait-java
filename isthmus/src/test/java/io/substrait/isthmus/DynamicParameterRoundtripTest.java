package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.expression.Expression;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.relation.Filter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.relation.RelProtoConverter;
import java.util.List;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

class DynamicParameterRoundtripTest extends PlanTestBase {
  static final String CREATES = "CREATE TABLE items (id INT, name VARCHAR, amount DOUBLE)";

  final Prepare.CatalogReader itemsCatalog;
  final RelCreator itemsRelCreator;
  final RelBuilder itemsBuilder;

  DynamicParameterRoundtripTest() throws SqlParseException {
    itemsCatalog = SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES);
    itemsRelCreator = new RelCreator(itemsCatalog);
    itemsBuilder = itemsRelCreator.createRelBuilder();
  }

  @Test
  void singleDynamicParamInFilter() {
    Rel table =
        sb.namedScan(
            List.of("items"), List.of("id", "name", "amount"), List.of(R.I32, N.STRING, R.FP64));

    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder().type(R.I32).parameterReference(0).build();

    Rel filtered = sb.filter(input -> sb.equal(sb.fieldReference(input, 0), dp), table);
    assertFullRoundTrip(filtered);
  }

  @Test
  void multipleDynamicParamsInFilter() {
    Rel table =
        sb.namedScan(
            List.of("items"), List.of("id", "name", "amount"), List.of(R.I32, N.STRING, R.FP64));

    Expression.DynamicParameter dpId =
        Expression.DynamicParameter.builder().type(R.I32).parameterReference(0).build();
    Expression.DynamicParameter dpName =
        Expression.DynamicParameter.builder().type(N.STRING).parameterReference(1).build();

    Rel filtered =
        sb.filter(
            input ->
                sb.and(
                    sb.equal(sb.fieldReference(input, 0), dpId),
                    sb.equal(sb.fieldReference(input, 1), dpName)),
            table);
    assertFullRoundTrip(filtered);
  }

  @Test
  void dynamicParamInProjection() {
    Rel table =
        sb.namedScan(
            List.of("items"), List.of("id", "name", "amount"), List.of(R.I32, N.STRING, R.FP64));

    Expression.DynamicParameter dpMultiplier =
        Expression.DynamicParameter.builder().type(R.FP64).parameterReference(0).build();

    Project project =
        sb.project(
            input -> List.of(sb.multiply(sb.fieldReference(input, 2), dpMultiplier)),
            Remap.of(List.of(3)),
            table);
    assertFullRoundTrip(project);
  }

  @Test
  void dynamicParamWithDifferentTypes() {
    Rel table =
        sb.namedScan(
            List.of("items"), List.of("id", "name", "amount"), List.of(R.I32, N.STRING, R.FP64));

    Expression.DynamicParameter dpInt =
        Expression.DynamicParameter.builder().type(R.I32).parameterReference(0).build();
    Expression.DynamicParameter dpString =
        Expression.DynamicParameter.builder().type(N.STRING).parameterReference(1).build();
    Expression.DynamicParameter dpDouble =
        Expression.DynamicParameter.builder().type(R.FP64).parameterReference(2).build();

    Project project =
        sb.project(input -> List.of(dpInt, dpString, dpDouble), Remap.of(List.of(3, 4, 5)), table);
    assertFullRoundTrip(project);
  }

  @Test
  void calciteDynamicParamToSubstraitAndBack() {
    RelDataType intType = itemsRelCreator.typeFactory().createSqlType(SqlTypeName.INTEGER);
    RexDynamicParam dynamicParam =
        (RexDynamicParam) itemsBuilder.getRexBuilder().makeDynamicParam(intType, 0);

    RelNode calcitePlan =
        itemsBuilder
            .scan("ITEMS")
            .filter(itemsBuilder.equals(itemsBuilder.field("ID"), dynamicParam))
            .build();

    Rel substraitRel = SubstraitRelVisitor.convert(calcitePlan, extensions);
    assertInstanceOf(Filter.class, substraitRel);
    Filter filter = (Filter) substraitRel;
    assertContainsDynamicParameter(filter.getCondition(), 0);

    ExtensionCollector collector = new ExtensionCollector();
    io.substrait.proto.Rel proto = new RelProtoConverter(collector).toProto(substraitRel);
    Rel roundtripped =
        new io.substrait.relation.ProtoRelConverter(collector, extensions).from(proto);
    assertEquals(substraitRel, roundtripped);
  }

  @Test
  void calciteMultipleDynamicParamsToSubstrait() {
    RelDataType intType = itemsRelCreator.typeFactory().createSqlType(SqlTypeName.INTEGER);
    RelDataType varcharType = itemsRelCreator.typeFactory().createSqlType(SqlTypeName.VARCHAR);

    RexDynamicParam idParam =
        (RexDynamicParam) itemsBuilder.getRexBuilder().makeDynamicParam(intType, 0);
    RexDynamicParam nameParam =
        (RexDynamicParam) itemsBuilder.getRexBuilder().makeDynamicParam(varcharType, 1);

    RelNode calcitePlan =
        itemsBuilder
            .scan("ITEMS")
            .filter(
                itemsBuilder.and(
                    itemsBuilder.equals(itemsBuilder.field("ID"), idParam),
                    itemsBuilder.equals(itemsBuilder.field("NAME"), nameParam)))
            .build();

    Rel substraitRel = SubstraitRelVisitor.convert(calcitePlan, extensions);
    assertInstanceOf(Filter.class, substraitRel);

    ExtensionCollector collector = new ExtensionCollector();
    io.substrait.proto.Rel proto = new RelProtoConverter(collector).toProto(substraitRel);
    Rel roundtripped =
        new io.substrait.relation.ProtoRelConverter(collector, extensions).from(proto);
    assertEquals(substraitRel, roundtripped);
  }

  @Test
  void fullCalciteRoundtripWithDynamicParam() {
    Rel table =
        sb.namedScan(
            List.of("items"), List.of("id", "name", "amount"), List.of(R.I32, N.STRING, R.FP64));

    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder().type(R.I32).parameterReference(0).build();

    Rel filtered = sb.filter(input -> sb.equal(sb.fieldReference(input, 0), dp), table);

    RelNode calciteNode = new SubstraitToCalcite(converterProvider).convert(filtered);
    assertInstanceOf(LogicalFilter.class, calciteNode);
    LogicalFilter calciteFilter = (LogicalFilter) calciteNode;
    assertContainsRexDynamicParam(calciteFilter.getCondition(), 0);

    Rel backToSubstrait = SubstraitRelVisitor.convert(calciteNode, extensions);
    assertEquals(filtered, backToSubstrait);
  }

  private void assertContainsDynamicParameter(Expression expr, int expectedRef) {
    if (!containsDynamicParameter(expr, expectedRef)) {
      throw new AssertionError(
          String.format(
              "Expected a DynamicParameter with ref=%d in expression: %s", expectedRef, expr));
    }
  }

  private boolean containsDynamicParameter(Expression expr, int expectedRef) {
    if (expr instanceof Expression.DynamicParameter) {
      return ((Expression.DynamicParameter) expr).parameterReference() == expectedRef;
    }
    if (expr instanceof Expression.ScalarFunctionInvocation) {
      Expression.ScalarFunctionInvocation sfi = (Expression.ScalarFunctionInvocation) expr;
      return sfi.arguments().stream()
          .filter(arg -> arg instanceof Expression)
          .anyMatch(arg -> containsDynamicParameter((Expression) arg, expectedRef));
    }
    return false;
  }

  private void assertContainsRexDynamicParam(RexNode rex, int expectedIndex) {
    if (!containsRexDynamicParam(rex, expectedIndex)) {
      throw new AssertionError(
          String.format("Expected a RexDynamicParam with index=%d in: %s", expectedIndex, rex));
    }
  }

  private boolean containsRexDynamicParam(RexNode rex, int expectedIndex) {
    if (rex instanceof RexDynamicParam) {
      return ((RexDynamicParam) rex).getIndex() == expectedIndex;
    }
    if (rex instanceof RexCall) {
      return ((RexCall) rex)
          .operands.stream().anyMatch(operand -> containsRexDynamicParam(operand, expectedIndex));
    }
    return false;
  }
}
