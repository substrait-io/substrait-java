package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Bindables.BindableTableScan;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class BindableTableScanTest extends PlanTestBase {
  private final CalciteSchema schema = CalciteSchema.createRootSchema(false);
  private final RelBuilder runtimeBuilder;
  private final TableScan scan;

  BindableTableScanTest() {
    schema.add("T", new RuntimeTable());
    runtimeBuilder = converterProvider.getRelBuilder(schema);
    scan = (TableScan) runtimeBuilder.scan("T").build();
  }

  private RelNode roundTrip(RelNode input) {
    Rel exported = SubstraitRelVisitor.convert(input, converterProvider);
    ExtensionCollector extensions = new ExtensionCollector();
    Rel decoded =
        new ProtoRelConverter(extensions, this.extensions)
            .from(new RelProtoConverter(extensions).toProto(exported));
    assertEquals(exported, decoded);
    return decoded.accept(
        converterProvider.getSubstraitRelNodeConverter(converterProvider.getRelBuilder(schema)),
        Context.newContext());
  }

  private static List<List<Object>> rows(RelNode input) throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    try (PreparedStatement statement = RelRunners.run(input);
        ResultSet result = statement.executeQuery()) {
      while (result.next()) {
        List<Object> row = new ArrayList<>();
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
          row.add(result.getObject(i));
        }
        rows.add(row);
      }
    }
    return rows;
  }

  @Test
  void preservesPlannerPushedFilter() throws Exception {
    RelNode filtered =
        runtimeBuilder
            .push(scan)
            .filter(runtimeBuilder.greaterThan(runtimeBuilder.field(0), runtimeBuilder.literal(5)))
            .build();
    BindableTableScan optimized =
        assertInstanceOf(BindableTableScan.class, plan(filtered, CoreRules.FILTER_SCAN));
    assertEquals(1, optimized.filters.size());
    assertEquals(List.of(List.of(6, 60, "six"), List.of(9, 90, "nine")), rows(filtered));
    assertEquals(rows(filtered), rows(roundTrip(optimized)));
  }

  static Stream<List<Integer>> projections() {
    return Stream.of(List.of(0, 1, 2), List.of(2), List.of(2, 0, 2), List.of());
  }

  @ParameterizedTest
  @MethodSource("projections")
  void filtersBeforeProjecting(List<Integer> projects) throws Exception {
    RelNode filtered =
        runtimeBuilder
            .push(scan)
            .filter(
                runtimeBuilder.greaterThan(runtimeBuilder.field(0), runtimeBuilder.literal(5)),
                runtimeBuilder.lessThan(runtimeBuilder.field(1), runtimeBuilder.literal(80)))
            .build();
    List<RexNode> filters =
        org.apache.calcite.plan.RelOptUtil.conjunctions(
            ((org.apache.calcite.rel.core.Filter) filtered).getCondition());
    BindableTableScan bindable =
        BindableTableScan.create(scan.getCluster(), scan.getTable(), filters, projects);
    NamedScan exported =
        assertInstanceOf(NamedScan.class, SubstraitRelVisitor.convert(bindable, converterProvider));
    assertEquals(
        TypeConverter.DEFAULT.toNamedStruct(scan.getRowType()), exported.getInitialSchema());
    assertTrue(exported.getFilter().isPresent());
    RelNode restored = roundTrip(bindable);
    assertEquals(
        bindable.getRowType().getFieldList().stream().map(f -> f.getType()).toList(),
        restored.getRowType().getFieldList().stream().map(f -> f.getType()).toList());
    Object[] row = {6, 60, "six"};
    if (projects.isEmpty()) {
      RelNode counted =
          runtimeBuilder
              .push(restored)
              .aggregate(runtimeBuilder.groupKey(), runtimeBuilder.countStar("C"))
              .build();
      assertEquals(List.of(List.of(1L)), rows(counted));
    } else {
      assertEquals(List.of(projects.stream().map(i -> row[i]).toList()), rows(restored));
    }
    assertEquals(2, bindable.filters.size());
    assertEquals(projects, bindable.projects);
  }

  @Test
  void preservesPlannerPushedProjection() throws Exception {
    RelNode projected =
        runtimeBuilder
            .push(scan)
            .filter(runtimeBuilder.greaterThan(runtimeBuilder.field(0), runtimeBuilder.literal(5)))
            .project(runtimeBuilder.field(2))
            .build();
    RelNode optimized = plan(projected, CoreRules.FILTER_SCAN, CoreRules.PROJECT_TABLE_SCAN);
    assertInstanceOf(BindableTableScan.class, optimized);
    assertEquals(rows(projected), rows(roundTrip(optimized)));
  }

  @Test
  void parentReferencesProjectedColumns() throws Exception {
    BindableTableScan bindable =
        BindableTableScan.create(scan.getCluster(), scan.getTable(), List.of(), List.of(2, 0));
    RelNode parent =
        runtimeBuilder
            .push(bindable)
            .filter(runtimeBuilder.greaterThan(runtimeBuilder.field(1), runtimeBuilder.literal(5)))
            .project(runtimeBuilder.field(0))
            .build();
    assertEquals(List.of(List.of("six"), List.of("nine")), rows(roundTrip(parent)));
  }

  @Test
  void preservesProjectionWithoutFilter() throws Exception {
    BindableTableScan bindable =
        BindableTableScan.create(scan.getCluster(), scan.getTable(), List.of(), List.of(2));
    assertEquals(
        List.of(List.of("one"), List.of("six"), List.of("nine"), List.of("missing")),
        rows(roundTrip(bindable)));
  }

  @Test
  void ordinaryScanIsUnchanged() throws Exception {
    assertEquals(rows(scan), rows(roundTrip(scan)));
    NamedScan exported =
        assertInstanceOf(NamedScan.class, SubstraitRelVisitor.convert(scan, converterProvider));
    assertTrue(exported.getFilter().isEmpty());
    assertTrue(exported.getRemap().isEmpty());
  }

  private RelNode update(RelNode input) {
    return LogicalTableModify.create(
        scan.getTable(),
        null,
        input,
        TableModify.Operation.UPDATE,
        List.of("V"),
        List.of(runtimeBuilder.literal(100)),
        false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsUpdateWithPushedFilter(boolean outerFilter) {
    RexNode predicate =
        runtimeBuilder.greaterThan(
            runtimeBuilder.getRexBuilder().makeInputRef(scan, 0), runtimeBuilder.literal(5));
    RelNode input =
        BindableTableScan.create(
            scan.getCluster(), scan.getTable(), List.of(predicate), List.of(0, 1, 2));
    if (outerFilter) {
      input =
          runtimeBuilder
              .push(input)
              .filter(runtimeBuilder.lessThan(runtimeBuilder.field(1), runtimeBuilder.literal(80)))
              .build();
    }
    RelNode modification = update(input);
    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modification, converterProvider));
    assertEquals("UPDATE cannot convert a scan with pushed filters", error.getMessage());
  }

  @Test
  void preservesUpdateOverUnfilteredBindableScan() {
    BindableTableScan input = BindableTableScan.create(scan.getCluster(), scan.getTable());
    NamedUpdate exported =
        assertInstanceOf(
            NamedUpdate.class, SubstraitRelVisitor.convert(update(input), converterProvider));
    assertEquals(ExpressionCreator.bool(false, true), exported.getCondition());
  }

  private static class RuntimeTable extends AbstractTable implements ProjectableFilterableTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      return factory
          .builder()
          .add("ID", SqlTypeName.INTEGER)
          .nullable(true)
          .add("V", SqlTypeName.INTEGER)
          .add("LABEL", SqlTypeName.VARCHAR, 20)
          .build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext context, List<RexNode> filters, int[] projects) {
      Object[][] data = {{1, 10, "one"}, {6, 60, "six"}, {9, 90, "nine"}, {null, 99, "missing"}};
      return Linq4j.asEnumerable(data)
          .select(
              row ->
                  projects == null ? row : Arrays.stream(projects).mapToObj(i -> row[i]).toArray());
    }
  }
}
