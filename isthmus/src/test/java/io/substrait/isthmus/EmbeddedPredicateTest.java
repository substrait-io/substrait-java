package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.calcite.rel.rules.VirtualTableExpansionRule;
import io.substrait.relation.Join;
import io.substrait.relation.Join.JoinType;
import io.substrait.relation.NamedScan;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class EmbeddedPredicateTest extends PlanTestBase {

  @Test
  void namedScanFiltersBeforeEmit() {
    NamedScan scan =
        sb.namedScan(List.of("example"), List.of("id", "keep"), List.of(R.I32, N.BOOLEAN));
    NamedScan filtered =
        NamedScan.builder()
            .from(scan)
            .filter(sb.fieldReference(scan, 1))
            .remap(sb.remap(0))
            .build();

    Project project = assertInstanceOf(Project.class, substraitToCalcite.convert(filtered));
    Filter filter = assertInstanceOf(Filter.class, project.getInput());
    assertInstanceOf(TableScan.class, filter.getInput());
    assertEquals(1, assertInstanceOf(RexInputRef.class, filter.getCondition()).getIndex());
    assertRowMatch(project.getRowType(), R.I32);
  }

  @Test
  void namedScanFalseAndNullFiltersProduceNoRows() {
    NamedScan scan = sb.namedScan(List.of("example"), List.of("id"), List.of(R.I32));
    for (Expression condition : List.of(sb.bool(false), ExpressionCreator.typedNull(N.BOOLEAN))) {
      NamedScan filtered = NamedScan.builder().from(scan).filter(condition).build();

      Values values = assertInstanceOf(Values.class, substraitToCalcite.convert(filtered));
      assertTrue(values.getTuples().isEmpty());
      assertRowMatch(values.getRowType(), R.I32);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void virtualTableFiltersLiteralAndComputedRowsBeforeEmit(boolean computed) {
    VirtualTableScan scan =
        VirtualTableScan.builder()
            .initialSchema(NamedStruct.of(List.of("id", "keep"), R.struct(R.I32, N.BOOLEAN)))
            .addRows(
                ExpressionCreator.nestedStruct(
                    false,
                    List.of(
                        computed ? sb.add(sb.i32(1), sb.i32(1)) : sb.i32(2),
                        ExpressionCreator.bool(true, true))),
                ExpressionCreator.nestedStruct(
                    false, List.of(sb.i32(3), ExpressionCreator.bool(true, false))),
                ExpressionCreator.nestedStruct(
                    false, List.of(sb.i32(4), ExpressionCreator.typedNull(N.BOOLEAN))))
            .build();
    VirtualTableScan filtered =
        VirtualTableScan.builder()
            .from(scan)
            .filter(sb.fieldReference(scan, 1))
            .remap(sb.remap(0))
            .build();

    assertEquals(List.of(List.of(2)), rows(substraitToCalcite.convert(filtered)));
  }

  @Test
  void bestEffortReadFilterMayBeIgnored() {
    VirtualTableScan scan =
        VirtualTableScan.builder().from(integers(1, 2)).bestEffortFilter(sb.bool(false)).build();

    assertEquals(List.of(List.of(1), List.of(2)), rows(substraitToCalcite.convert(scan)));
  }

  @ParameterizedTest
  @EnumSource(
      value = JoinType.class,
      names = {"LEFT", "RIGHT", "OUTER"})
  void postJoinFilterSeesNullExtendedRows(JoinType joinType) {
    Join join = equalityJoin(joinType);
    int nullField = joinType == JoinType.RIGHT ? 0 : 1;
    Join filtered =
        Join.builder()
            .from(join)
            .postJoinFilter(sb.isNull(sb.fieldReference(join, nullField)))
            .build();

    RelNode converted = substraitToCalcite.convert(filtered);
    Filter filter = assertInstanceOf(Filter.class, converted);
    assertInstanceOf(org.apache.calcite.rel.core.Join.class, filter.getInput());
    List<Object> expected =
        joinType == JoinType.RIGHT ? Arrays.asList(null, 3) : Arrays.asList(1, null);
    assertEquals(List.of(expected), rows(converted));
  }

  @Test
  void postJoinFilterRunsBeforeEmit() {
    Join join = equalityJoin(JoinType.LEFT);
    Join filtered =
        Join.builder()
            .from(join)
            .postJoinFilter(sb.isNull(sb.fieldReference(join, 1)))
            .remap(sb.remap(0))
            .build();

    assertEquals(List.of(List.of(1)), rows(substraitToCalcite.convert(filtered)));
  }

  @ParameterizedTest
  @EnumSource(
      value = JoinType.class,
      names = {"LEFT", "RIGHT", "OUTER"})
  void protoPostJoinFilterPreservesUnmatchedRows(JoinType joinType) {
    Join join = equalityJoin(joinType);
    Join filtered =
        Join.builder()
            .from(join)
            .postJoinFilter(
                sb.or(sb.isNull(sb.fieldReference(join, 0)), sb.isNull(sb.fieldReference(join, 1))))
            .remap(sb.remap(1, 0))
            .build();
    ExtensionCollector collector = new ExtensionCollector();
    io.substrait.proto.Rel proto = new RelProtoConverter(collector).toProto(filtered);
    Rel decoded = new ProtoRelConverter(collector, extensions).from(proto);

    List<List<Object>> expected =
        joinType == JoinType.LEFT
            ? List.of(Arrays.asList(null, 1))
            : joinType == JoinType.RIGHT
                ? List.of(Arrays.asList(3, null))
                : List.of(Arrays.asList(null, 1), Arrays.asList(3, null));
    assertEquals(expected, rows(substraitToCalcite.convert(decoded)));
  }

  @ParameterizedTest
  @EnumSource(
      value = JoinType.class,
      names = {"INNER", "LEFT", "LEFT_SEMI", "LEFT_ANTI"})
  void falseAndNullPostJoinFiltersProduceNoRows(JoinType joinType) {
    Join join = equalityJoin(joinType);
    for (Expression condition : List.of(sb.bool(false), ExpressionCreator.typedNull(N.BOOLEAN))) {
      Join filtered = Join.builder().from(join).postJoinFilter(condition).build();

      assertEquals(List.of(), rows(substraitToCalcite.convert(filtered)));
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = JoinType.class,
      names = {"LEFT_SEMI", "LEFT_ANTI"})
  void postJoinFilterUsesSemiAndAntiOutput(JoinType joinType) {
    Join join = equalityJoin(joinType);
    Join filtered =
        Join.builder()
            .from(join)
            .postJoinFilter(sb.equal(sb.fieldReference(join, 0), sb.i32(1)))
            .build();

    assertEquals(
        joinType == JoinType.LEFT_ANTI ? List.of(List.of(1)) : List.of(),
        rows(substraitToCalcite.convert(filtered)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void readFilterRetainsEnclosingCorrelation(boolean named) {
    Rel outer = integers(1, 2).withRelAnchor(7);
    FieldReference outerRef =
        FieldReference.newRootStructOuterReferenceByRelReference(0, outer.getRecordType(), 7);
    VirtualTableScan virtual = integers(1, 2);
    Expression condition = sb.equal(sb.fieldReference(virtual, 0), outerRef);
    Rel inner =
        named
            ? NamedScan.builder()
                .initialSchema(virtual.getInitialSchema())
                .addNames("example")
                .filter(condition)
                .build()
            : VirtualTableScan.builder().from(virtual).filter(condition).build();
    Rel plan = sb.filter(input -> sb.exists(inner), outer);

    Filter converted = assertInstanceOf(Filter.class, substraitToCalcite.convert(plan));
    assertFalse(converted.getVariablesSet().isEmpty());
    RexSubQuery exists = assertInstanceOf(RexSubQuery.class, converted.getCondition());
    Filter innerFilter = assertInstanceOf(Filter.class, exists.rel);
    assertTrue(innerFilter.getCondition().toString().contains("$cor"));
  }

  @Test
  void postJoinFilterRetainsEnclosingCorrelation() {
    Rel outer = integers(1, 2).withRelAnchor(7);
    FieldReference outerRef =
        FieldReference.newRootStructOuterReferenceByRelReference(0, outer.getRecordType(), 7);
    Join join = equalityJoin(JoinType.INNER);
    Join inner =
        Join.builder()
            .from(join)
            .postJoinFilter(sb.equal(sb.fieldReference(join, 0), outerRef))
            .build();

    Rel plan = sb.filter(input -> sb.exists(inner), outer);
    Filter converted = assertInstanceOf(Filter.class, substraitToCalcite.convert(plan));
    assertFalse(converted.getVariablesSet().isEmpty());
    RexSubQuery exists = assertInstanceOf(RexSubQuery.class, converted.getCondition());
    Filter innerFilter = assertInstanceOf(Filter.class, exists.rel);
    assertInstanceOf(org.apache.calcite.rel.core.Join.class, innerFilter.getInput());
    assertTrue(innerFilter.getCondition().toString().contains("$cor"));
  }

  private Join equalityJoin(JoinType joinType) {
    return sb.join(
        input -> sb.equal(sb.fieldReference(input, 0), sb.fieldReference(input, 1)),
        joinType,
        integers(1, 2),
        integers(2, 3));
  }

  private VirtualTableScan integers(int... values) {
    return VirtualTableScan.builder()
        .initialSchema(NamedStruct.of(List.of("id"), R.struct(R.I32)))
        .rows(
            Arrays.stream(values)
                .mapToObj(value -> ExpressionCreator.nestedStruct(false, List.of(sb.i32(value))))
                .collect(Collectors.toList()))
        .build();
  }

  private List<List<Object>> rows(RelNode rel) {
    DataContext dataContext =
        new DataContext() {
          @Override
          public SchemaPlus getRootSchema() {
            return Frameworks.createRootSchema(true);
          }

          @Override
          public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
          }

          @Override
          public QueryProvider getQueryProvider() {
            return null;
          }

          @Override
          public Object get(String name) {
            return null;
          }
        };
    RelNode executable = plan(rel, VirtualTableExpansionRule.instance());
    try (Interpreter interpreter = new Interpreter(dataContext, executable)) {
      return interpreter.toList().stream().map(Arrays::asList).collect(Collectors.toList());
    }
  }
}
