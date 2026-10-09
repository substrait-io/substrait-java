package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Message;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.TypeCreator;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class UpdateConversionTest {
  private final ConverterProvider provider = ConverterProvider.DEFAULT;
  private final Prepare.CatalogReader catalog =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog(
          provider,
          "CREATE TABLE src1 (intcol INT, charcol VARCHAR(10))",
          "CREATE TABLE src2 (intcol INT, charcol VARCHAR(10))");

  UpdateConversionTest() throws SqlParseException {}

  @ParameterizedTest
  @ValueSource(strings = {"charcol = 'a'", "1 = 0", "intcol > 0 AND charcol IS NOT NULL"})
  void preservesWhereClause(String predicate) throws SqlParseException {
    NamedUpdate update =
        assertInstanceOf(
            NamedUpdate.class, convert("UPDATE src1 SET intcol = intcol + 1 WHERE " + predicate));
    Filter select =
        assertInstanceOf(Filter.class, convert("SELECT * FROM src1 WHERE " + predicate));

    assertEquals(select.getCondition(), update.getCondition());
    Project value = assertInstanceOf(Project.class, convert("SELECT intcol + 1 FROM src1"));
    assertEquals(
        value.getExpressions().get(0), update.getTransformations().get(0).getTransformation());
  }

  @Test
  void preservesUnconditionalUpdate() throws SqlParseException {
    NamedUpdate update =
        assertInstanceOf(NamedUpdate.class, convert("UPDATE src1 SET intcol = 10"));

    assertEquals(ExpressionCreator.bool(false, true), update.getCondition());
  }

  @Test
  void resolvesPredicatesAndAssignmentsThroughNestedProjections() throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = intcol + 1 WHERE charcol = 'a'");
    RelNode input = original.getInput();
    RexBuilder rexBuilder = original.getCluster().getRexBuilder();
    LogicalProject reordered =
        LogicalProject.create(
            input,
            List.of(),
            List.of(
                rexBuilder.makeInputRef(input, 1),
                rexBuilder.makeInputRef(input, 2),
                rexBuilder.makeInputRef(input, 0)),
            List.of("c", "next_value", "previous_value"));
    RexNode nextValue = rexBuilder.makeInputRef(reordered, 1);
    LogicalFilter filtered =
        LogicalFilter.create(
            reordered,
            rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                nextValue,
                rexBuilder.makeExactLiteral(BigDecimal.TEN, nextValue.getType())));
    RexNode assignment =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            nextValue,
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(2), nextValue.getType()));
    TableModify modification =
        LogicalTableModify.create(
            original.getTable(),
            original.getCatalogReader(),
            filtered,
            TableModify.Operation.UPDATE,
            original.getUpdateColumnList(),
            List.of(assignment),
            false);

    NamedUpdate update =
        assertInstanceOf(NamedUpdate.class, SubstraitRelVisitor.convert(modification, provider));
    Project expected =
        assertInstanceOf(
            Project.class,
            // The lower filter comes first, so that it still guards the filter above it.
            convert("SELECT (intcol + 1) + 2 FROM src1 WHERE charcol = 'a' AND intcol + 1 > 10"));

    assertEquals(
        assertInstanceOf(Filter.class, expected.getInput()).getCondition(), update.getCondition());
    assertEquals(
        expected.getExpressions().get(0), update.getTransformations().get(0).getTransformation());
  }

  @Test
  void rejectsUnsupportedRowSelection() throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    LogicalSort limited =
        LogicalSort.create(
            original.getInput(),
            RelCollations.EMPTY,
            null,
            original.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.ONE));
    RelNode modification = original.copy(original.getTraitSet(), List.of(limited));

    assertThrows(
        UnsupportedOperationException.class,
        () -> SubstraitRelVisitor.convert(modification, provider));
  }

  @Test
  void rejectsScanOfAnotherTable() throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RelNode modification =
        original.copy(
            original.getTraitSet(),
            List.of(modification("UPDATE src2 SET intcol = 10").getInput()));

    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modification, provider));
    assertEquals(
        "UPDATE requires a scan of its target table beneath projections and filters",
        error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "UPDATE src1 SET intcol = 10 WHERE EXISTS"
            + " (SELECT 1 FROM src1 AS other WHERE other.intcol = src1.intcol)",
        "UPDATE src1 SET intcol ="
            + " (SELECT MAX(other.intcol) FROM src1 AS other WHERE other.charcol = src1.charcol)",
        "UPDATE src1 SET intcol ="
            + " (SELECT MAX(other.intcol) FROM src1 AS other WHERE other.charcol = src1.charcol)"
            + " WHERE EXISTS (SELECT 1 FROM src1 AS other WHERE other.intcol = src1.intcol)"
      })
  void preservesCorrelationBindingsInTableCoordinates(String sql) throws SqlParseException {
    Plan plan = new SqlToSubstrait(provider).convert(sql, catalog);
    io.substrait.proto.Plan encoded = new PlanProtoConverter().toProto(plan);
    io.substrait.proto.UpdateRel update = encoded.getRelations(0).getRoot().getInput().getUpdate();
    assertTrue(update.getCommon().hasRelAnchor());
    int anchor = update.getCommon().getRelAnchor();
    int referenceCount =
        (sql.contains(" WHERE EXISTS") ? 1 : 0) + (sql.contains("SELECT MAX") ? 1 : 0);
    assertEquals(Collections.nCopies(referenceCount, anchor), outerReferences(update));
    Plan decoded = new ProtoPlanConverter().from(encoded);
    assertEquals(encoded, new PlanProtoConverter().toProto(decoded));
    RelNode restored =
        new SubstraitToCalcite(provider, catalog).convert(decoded.getRoots().get(0).getInput());
    Plan reconverted =
        Plan.builder()
            .from(plan)
            .roots(
                List.of(
                    Plan.Root.builder()
                        .from(plan.getRoots().get(0))
                        .input(SubstraitRelVisitor.convert(restored, provider))
                        .build()))
            .build();
    assertEquals(encoded, new PlanProtoConverter().toProto(reconverted));
  }

  private List<Integer> outerReferences(Message message) {
    List<Integer> anchors = new ArrayList<>();
    if (message instanceof io.substrait.proto.Expression.FieldReference.OuterReference) {
      anchors.add(
          ((io.substrait.proto.Expression.FieldReference.OuterReference) message)
              .getRelReference());
    }
    for (Object field : message.getAllFields().values()) {
      for (Object value : field instanceof List<?> ? (List<?>) field : List.of(field)) {
        if (value instanceof Message) {
          anchors.addAll(outerReferences((Message) value));
        }
      }
    }
    return anchors;
  }

  @Test
  void rejectsCorrelationBindingOnProjection() throws SqlParseException {
    TableModify original =
        modification(
            "UPDATE src1 SET intcol = 10 WHERE EXISTS"
                + " (SELECT 1 FROM src1 AS other WHERE other.intcol = src1.intcol)");
    org.apache.calcite.rel.core.Project source =
        assertInstanceOf(org.apache.calcite.rel.core.Project.class, original.getInput());
    org.apache.calcite.rel.core.Filter filter =
        assertInstanceOf(org.apache.calcite.rel.core.Filter.class, source.getInput());
    RelNode scan = filter.getInput();
    RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
    LogicalProject projected =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1)),
            scan.getRowType());
    RelNode filtered = filter.copy(filter.getTraitSet(), projected, filter.getCondition());
    RelNode modified =
        original.copy(
            original.getTraitSet(), List.of(source.copy(source.getTraitSet(), List.of(filtered))));

    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modified, provider));
    assertEquals(
        "UPDATE cannot remove an input that binds a correlated subquery", error.getMessage());
  }

  @Test
  void keepsBindingOfSubqueryThatReusesTargetScan() throws SqlParseException {
    org.apache.calcite.rel.core.Project correlated =
        assertInstanceOf(
            org.apache.calcite.rel.core.Project.class,
            SubstraitSqlToCalcite.convertQuery(
                    "SELECT 1 FROM src1 AS other WHERE EXISTS"
                        + " (SELECT 1 FROM src1 AS inner_row WHERE inner_row.intcol = other.intcol)",
                    catalog,
                    provider)
                .rel);
    // The subquery binds its correlation to a scan that the UPDATE input also uses.
    RelNode scan =
        assertInstanceOf(org.apache.calcite.rel.core.Filter.class, correlated.getInput())
            .getInput();
    TableModify modification =
        LogicalTableModify.create(
            scan.getTable(),
            catalog,
            LogicalFilter.create(scan, RexSubQuery.exists(correlated)),
            TableModify.Operation.UPDATE,
            scan.getRowType().getFieldNames().subList(0, 1),
            List.of(scan.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.TEN)),
            false);

    io.substrait.proto.UpdateRel update =
        new RelProtoConverter(new ExtensionCollector())
            .toProto(SubstraitRelVisitor.convert(modification, provider))
            .getUpdate();

    assertFalse(update.getCommon().hasRelAnchor());
    assertEquals(1, outerReferences(update).size());
  }

  @Test
  void rejectsVirtualColumnScanWithDifferentSchema() throws SqlParseException {
    CalciteCatalogReader virtualCatalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(provider);
    virtualCatalog
        .getRootSchema()
        .add(
            "vt",
            new AbstractTable() {
              @Override
              public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory
                    .builder()
                    .add("a", SqlTypeName.INTEGER)
                    .add("v", SqlTypeName.INTEGER)
                    .add("b", SqlTypeName.INTEGER)
                    .build();
              }

              @Override
              public <T> T unwrap(Class<T> type) {
                NullInitializerExpressionFactory initializer =
                    new NullInitializerExpressionFactory() {
                      @Override
                      public ColumnStrategy generationStrategy(RelOptTable table, int column) {
                        return column == 1 ? ColumnStrategy.VIRTUAL : ColumnStrategy.NOT_NULLABLE;
                      }

                      @Override
                      public RexNode newColumnDefaultValue(
                          RelOptTable table, int column, InitializerContext context) {
                        return context.getRexBuilder().makeExactLiteral(BigDecimal.TEN);
                      }
                    };
                return type.isInstance(initializer) ? type.cast(initializer) : super.unwrap(type);
              }
            });
    RelNode modification =
        SubstraitSqlToCalcite.convertQuery(
                "UPDATE vt SET a = b WHERE b > 0", virtualCatalog, provider)
            .rel;

    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modification, provider));
    assertEquals(
        "UPDATE target scan schema must match the target table schema", error.getMessage());
  }

  @Test
  void preservesUncorrelatedSubquery() throws SqlParseException {
    NamedUpdate update =
        assertInstanceOf(
            NamedUpdate.class,
            convert("UPDATE src1 SET intcol = 10 WHERE EXISTS (SELECT 1 FROM src1 AS other)"));
    Filter expected =
        assertInstanceOf(
            Filter.class, convert("SELECT * FROM src1 WHERE EXISTS (SELECT 1 FROM src1 AS other)"));

    assertEquals(expected.getCondition(), update.getCondition());
    assertNotNull(new SubstraitToCalcite(provider, catalog).convert(update));
  }

  @Test
  void preservesWindowAssignmentWithoutFilter() throws SqlParseException {
    NamedUpdate update =
        assertInstanceOf(
            NamedUpdate.class,
            convert("UPDATE src1 SET intcol = ROW_NUMBER() OVER (ORDER BY charcol)"));
    Project expected =
        assertInstanceOf(
            Project.class,
            convert("SELECT CAST(ROW_NUMBER() OVER (ORDER BY charcol) AS INT) FROM src1"));

    assertEquals(ExpressionCreator.bool(false, true), update.getCondition());
    assertEquals(
        expected.getExpressions().get(0), update.getTransformations().get(0).getTransformation());
    assertNotNull(new SubstraitToCalcite(provider, catalog).convert(update));
  }

  @Test
  void rejectsWindowAssignmentOnFilteredRows() {
    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                convert(
                    "UPDATE src1 SET intcol = ROW_NUMBER() OVER (ORDER BY charcol)"
                        + " WHERE intcol > 10"));
    assertEquals("UPDATE cannot apply a window assignment to filtered rows", error.getMessage());
  }

  @Test
  void rejectsWindowProjection() throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RelNode window =
        SubstraitSqlToCalcite.convertQuery(
                "SELECT intcol, charcol, ROW_NUMBER() OVER (ORDER BY intcol) AS rn"
                    + " FROM src1 WHERE intcol > 10",
                catalog,
                provider)
            .rel;
    TableModify modification =
        LogicalTableModify.create(
            original.getTable(),
            original.getCatalogReader(),
            window,
            TableModify.Operation.UPDATE,
            original.getUpdateColumnList(),
            List.of(original.getCluster().getRexBuilder().makeInputRef(window, 2)),
            false);

    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modification, provider));
    assertEquals("UPDATE cannot apply a window assignment to filtered rows", error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"filter above", "duplicated", "nested"})
  void rejectsWindowProjectionThatIsNotOneAssignment(String use) throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    org.apache.calcite.rel.core.Project nested =
        assertInstanceOf(
            org.apache.calcite.rel.core.Project.class,
            SubstraitSqlToCalcite.convertQuery(
                    "SELECT MAX(rn) OVER () FROM (SELECT intcol, charcol,"
                        + " ROW_NUMBER() OVER (ORDER BY intcol) AS rn FROM src1) AS numbered",
                    catalog,
                    provider)
                .rel);
    RelNode window = nested.getInput();
    assertTrue(assertInstanceOf(org.apache.calcite.rel.core.Project.class, window).containsOver());
    RexBuilder rexBuilder = window.getCluster().getRexBuilder();
    RexNode rowNumber = rexBuilder.makeInputRef(window, window.getRowType().getFieldCount() - 1);
    RelNode input = window;
    RexNode assignment;
    if ("filter above".equals(use)) {
      input =
          LogicalFilter.create(
              window,
              rexBuilder.makeCall(
                  SqlStdOperatorTable.GREATER_THAN,
                  rowNumber,
                  rexBuilder.makeZeroLiteral(rowNumber.getType())));
      assignment = rexBuilder.makeExactLiteral(BigDecimal.TEN);
    } else if ("duplicated".equals(use)) {
      assignment = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, rowNumber, rowNumber);
    } else {
      assignment = nested.getProjects().get(0);
    }
    TableModify modification =
        LogicalTableModify.create(
            original.getTable(),
            original.getCatalogReader(),
            input,
            TableModify.Operation.UPDATE,
            original.getUpdateColumnList(),
            List.of(assignment),
            false);

    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modification, provider));
    assertEquals("UPDATE cannot flatten a window projection", error.getMessage());
  }

  @Test
  void rejectsWindowFilterAboveAnotherFilter() throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    org.apache.calcite.rel.core.Project window =
        assertInstanceOf(
            org.apache.calcite.rel.core.Project.class,
            SubstraitSqlToCalcite.convertQuery(
                    "SELECT COUNT(*) OVER () FROM src1", catalog, provider)
                .rel);
    RelNode scan = window.getInput();
    RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
    LogicalFilter lower =
        LogicalFilter.create(
            scan,
            rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeInputRef(scan, 0),
                rexBuilder.makeExactLiteral(BigDecimal.ZERO)));
    LogicalFilter upper =
        LogicalFilter.create(
            lower,
            rexBuilder.makeCall(
                SqlStdOperatorTable.LESS_THAN,
                window.getProjects().get(0),
                rexBuilder.makeExactLiteral(BigDecimal.TEN)));
    TableModify modification =
        LogicalTableModify.create(
            original.getTable(),
            original.getCatalogReader(),
            upper,
            TableModify.Operation.UPDATE,
            original.getUpdateColumnList(),
            List.of(rexBuilder.makeExactLiteral(BigDecimal.TEN)),
            false);

    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modification, provider));
    assertEquals("UPDATE cannot merge a window filter with a lower filter", error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " WHERE v > 0"})
  void preservesSingleUseNondeterministicAssignment(String where) throws SqlParseException {
    ConverterProvider customProvider = randomProvider();
    Prepare.CatalogReader doubleCatalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            customProvider, "CREATE TABLE doubles (v DOUBLE)");
    NamedUpdate update =
        assertInstanceOf(
            NamedUpdate.class,
            new SqlToSubstrait(customProvider)
                .convert("UPDATE doubles SET v = RAND()" + where, doubleCatalog)
                .getRoots()
                .get(0)
                .getInput());

    Expression.ScalarFunctionInvocation random =
        assertInstanceOf(
            Expression.ScalarFunctionInvocation.class,
            update.getTransformations().get(0).getTransformation());
    assertEquals("random", random.declaration().name());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsDuplicatingProjectedSubqueryBindings(boolean filterReference)
      throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RelNode input = original.getInput();
    RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    RexNode scalar =
        RexSubQuery.scalar(
            SubstraitSqlToCalcite.convertQuery(
                    "SELECT MAX(a.intcol) FROM src1 a WHERE EXISTS"
                        + " (SELECT 1 FROM src1 b WHERE b.charcol = a.charcol)",
                    catalog,
                    provider)
                .rel);
    LogicalProject projected =
        LogicalProject.create(input, List.of(), List.of(scalar), List.of("next_value"));
    RexNode value = rexBuilder.makeInputRef(projected, 0);
    RelNode selected =
        filterReference
            ? LogicalFilter.create(
                projected,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN,
                    value,
                    rexBuilder.makeExactLiteral(BigDecimal.ZERO)))
            : projected;
    RexNode assignment =
        filterReference ? value : rexBuilder.makeCall(SqlStdOperatorTable.PLUS, value, value);
    TableModify modification =
        LogicalTableModify.create(
            original.getTable(),
            original.getCatalogReader(),
            selected,
            TableModify.Operation.UPDATE,
            original.getUpdateColumnList(),
            List.of(assignment),
            false);

    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modification, provider));
    assertEquals("UPDATE cannot duplicate a projected subquery", error.getMessage());
  }

  @Test
  void rejectsExponentialProjectionInlining() throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RelNode input = original.getInput();
    RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    for (int i = 0; i < 10; i++) {
      RexNode value = rexBuilder.makeInputRef(input, 0);
      input =
          LogicalProject.create(
              input,
              List.of(),
              List.of(rexBuilder.makeCall(SqlStdOperatorTable.PLUS, value, value)),
              List.of("next_value"));
    }
    TableModify modification =
        LogicalTableModify.create(
            original.getTable(),
            original.getCatalogReader(),
            input,
            TableModify.Operation.UPDATE,
            original.getUpdateColumnList(),
            List.of(rexBuilder.makeInputRef(input, 0)),
            false);

    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modification, provider));
    assertEquals(
        "UPDATE projection flattening would exceed the expression complexity limit",
        error.getMessage());
  }

  @ParameterizedTest
  @CsvSource({
    "'SELECT intcol FROM src1 FETCH NEXT 1 ROWS ONLY', false",
    "'SELECT intcol FROM src1 ORDER BY intcol FETCH NEXT 1 ROWS ONLY', false",
    "'SELECT n FROM (VALUES (-1), (1)) AS t(n) OFFSET 1 ROWS', false",
    "'SELECT n FROM (VALUES (-1), (1)) AS t(n) ORDER BY n OFFSET 1 ROWS', true",
    "'SELECT n FROM (VALUES (1), (2)) AS t(n) ORDER BY n FETCH NEXT 1 ROWS ONLY', true"
  })
  void checksOrderingOfLimitedSubqueriesInMergedFilters(String sql, boolean deterministic)
      throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RexBuilder rexBuilder = original.getCluster().getRexBuilder();
    RexNode scalar =
        RexSubQuery.scalar(SubstraitSqlToCalcite.convertQuery(sql, catalog, provider).rel);
    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN, scalar, rexBuilder.makeExactLiteral(BigDecimal.ZERO));
    LogicalFilter filtered =
        LogicalFilter.create(LogicalFilter.create(original.getInput(), predicate), predicate);
    RelNode modification = original.copy(original.getTraitSet(), List.of(filtered));

    if (deterministic) {
      assertInstanceOf(NamedUpdate.class, SubstraitRelVisitor.convert(modification, provider));
    } else {
      UnsupportedOperationException error =
          assertThrows(
              UnsupportedOperationException.class,
              () -> SubstraitRelVisitor.convert(modification, provider));
      assertEquals("UPDATE cannot merge nondeterministic filters", error.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void rejectsDuplicatingNondeterministicProjectedValues(int subqueryDepth)
      throws SqlParseException {
    ConverterProvider customProvider = randomProvider();
    Prepare.CatalogReader doubleCatalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            customProvider, "CREATE TABLE doubles (v DOUBLE)");
    TableModify original =
        assertInstanceOf(
            TableModify.class,
            SubstraitSqlToCalcite.convertQuery(
                    "UPDATE doubles SET v = 10", doubleCatalog, customProvider)
                .rel);
    RelNode scan =
        assertInstanceOf(org.apache.calcite.rel.core.Project.class, original.getInput()).getInput();
    RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
    RexNode randomCall = rexBuilder.makeCall(SqlStdOperatorTable.RAND);
    assertInstanceOf(
        Expression.ScalarFunctionInvocation.class,
        randomCall.accept(customProvider.getRexExpressionConverter(null)));
    RexNode projectedValue = wrapInScalarSubqueries(scan, randomCall, subqueryDepth);
    LogicalProject projected =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(scan, 0), projectedValue),
            List.of("v", "random_value"));
    RexNode value = rexBuilder.makeInputRef(projected, 1);
    // Subtracting the same projected value must produce zero, not two independent random calls.
    RexNode assignment = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, value, value);
    TableModify modification =
        LogicalTableModify.create(
            original.getTable(),
            original.getCatalogReader(),
            projected,
            TableModify.Operation.UPDATE,
            original.getUpdateColumnList(),
            List.of(assignment),
            false);

    assertThrows(
        UnsupportedOperationException.class,
        () -> SubstraitRelVisitor.convert(modification, customProvider));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void rejectsMergingNondeterministicFilters(int subqueryDepth) throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RexNode predicate = randomPredicate(original.getInput(), subqueryDepth);
    LogicalFilter filtered =
        LogicalFilter.create(LogicalFilter.create(original.getInput(), predicate), predicate);
    RelNode modification = original.copy(original.getTraitSet(), List.of(filtered));

    assertThrows(
        UnsupportedOperationException.class,
        () -> SubstraitRelVisitor.convert(modification, randomProvider()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preservesSingleNondeterministicFilter(boolean repeatedPredicate) throws SqlParseException {
    ConverterProvider customProvider = randomProvider();
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RexBuilder rexBuilder = original.getCluster().getRexBuilder();
    RexNode predicate = randomPredicate(original.getInput(), 0);
    RexNode condition =
        repeatedPredicate
            ? rexBuilder.makeCall(SqlStdOperatorTable.AND, predicate, predicate)
            : predicate;
    LogicalFilter filtered = LogicalFilter.create(original.getInput(), condition);
    RelNode modification = original.copy(original.getTraitSet(), List.of(filtered));

    NamedUpdate update =
        assertInstanceOf(
            NamedUpdate.class, SubstraitRelVisitor.convert(modification, customProvider));

    assertEquals(
        condition.accept(customProvider.getRexExpressionConverter(null)), update.getCondition());
    if (repeatedPredicate) {
      Expression.ScalarFunctionInvocation conjunction =
          assertInstanceOf(Expression.ScalarFunctionInvocation.class, update.getCondition());
      assertEquals(2, conjunction.arguments().size());
      conjunction
          .arguments()
          .forEach(
              argument -> {
                Expression.ScalarFunctionInvocation comparison =
                    assertInstanceOf(Expression.ScalarFunctionInvocation.class, argument);
                Expression.ScalarFunctionInvocation random =
                    assertInstanceOf(
                        Expression.ScalarFunctionInvocation.class, comparison.arguments().get(0));
                assertEquals("random", random.declaration().name());
              });
    }
  }

  @Test
  void preservesDeterministicSubqueriesInProjectionsAndFilterChains() throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RelNode input = original.getInput();
    RexBuilder rexBuilder = original.getCluster().getRexBuilder();
    RexNode scalar = wrapInScalarSubqueries(input, rexBuilder.makeExactLiteral(BigDecimal.TEN), 2);
    LogicalProject projected =
        LogicalProject.create(
            input,
            List.of(),
            List.of(rexBuilder.makeInputRef(input, 0), rexBuilder.makeInputRef(input, 1), scalar),
            List.of("intcol", "charcol", "next_value"));
    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(projected, 0),
            rexBuilder.makeExactLiteral(BigDecimal.ZERO));
    LogicalFilter filtered =
        LogicalFilter.create(LogicalFilter.create(projected, predicate), predicate);
    TableModify modification =
        LogicalTableModify.create(
            original.getTable(),
            original.getCatalogReader(),
            filtered,
            TableModify.Operation.UPDATE,
            original.getUpdateColumnList(),
            List.of(rexBuilder.makeInputRef(filtered, 2)),
            false);

    NamedUpdate update =
        assertInstanceOf(NamedUpdate.class, SubstraitRelVisitor.convert(modification, provider));

    assertInstanceOf(
        Expression.ScalarSubquery.class, update.getTransformations().get(0).getTransformation());
    assertNotNull(new SubstraitToCalcite(provider, catalog).convert(update));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void checksDeterminismInsideAggregateSubqueries(boolean nondeterministic)
      throws SqlParseException {
    ConverterProvider customProvider = randomProvider();
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RexBuilder rexBuilder = original.getCluster().getRexBuilder();
    RexNode scalar =
        RexSubQuery.scalar(
            SubstraitSqlToCalcite.convertQuery(
                    "SELECT MAX(" + (nondeterministic ? "RAND()" : "intcol") + ") FROM src1",
                    catalog,
                    customProvider)
                .rel);
    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN, scalar, rexBuilder.makeZeroLiteral(scalar.getType()));
    LogicalFilter filtered =
        LogicalFilter.create(LogicalFilter.create(original.getInput(), predicate), predicate);
    RelNode modification = original.copy(original.getTraitSet(), List.of(filtered));

    if (nondeterministic) {
      assertThrows(
          UnsupportedOperationException.class,
          () -> SubstraitRelVisitor.convert(modification, customProvider));
    } else {
      NamedUpdate update =
          assertInstanceOf(
              NamedUpdate.class, SubstraitRelVisitor.convert(modification, customProvider));
      assertNotNull(new SubstraitToCalcite(customProvider, catalog).convert(update));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"aggregate function", "aggregate argument", "window function"})
  void checksDeterminismOfAggregateAndWindowFunctions(String nondeterministic)
      throws SqlParseException {
    TableModify original = modification("UPDATE src1 SET intcol = 10");
    RelNode scan =
        assertInstanceOf(org.apache.calcite.rel.core.Project.class, original.getInput()).getInput();
    RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
    RexNode selected =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(scan, 0),
            rexBuilder.makeExactLiteral(BigDecimal.ZERO));
    SqlAggFunction volatileCount =
        new SqlAggFunction(
            "VOLATILE_COUNT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT,
            null,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION) {
          @Override
          public boolean isDeterministic() {
            return false;
          }
        };
    RexNode value;
    if ("window function".equals(nondeterministic)) {
      RexNode count =
          assertInstanceOf(
                  org.apache.calcite.rel.core.Project.class,
                  SubstraitSqlToCalcite.convertQuery(
                          "SELECT COUNT(*) OVER () FROM src1", catalog, provider)
                      .rel)
              .getProjects()
              .get(0);
      value =
          count.accept(
              new RexShuttle() {
                @Override
                public SqlAggFunction visitOverAggFunction(SqlAggFunction function) {
                  return volatileCount;
                }
              });
    } else {
      boolean function = "aggregate function".equals(nondeterministic);
      value =
          RexSubQuery.scalar(
              LogicalAggregate.create(
                  scan,
                  List.of(),
                  ImmutableBitSet.of(),
                  null,
                  List.of(
                      AggregateCall.create(
                          function ? volatileCount : SqlStdOperatorTable.COUNT,
                          false,
                          false,
                          false,
                          function
                              ? List.of()
                              : List.of(rexBuilder.makeCall(SqlStdOperatorTable.RAND)),
                          List.of(),
                          -1,
                          null,
                          RelCollations.EMPTY,
                          0,
                          scan,
                          null,
                          "c"))));
    }
    RexNode volatilePredicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN, value, rexBuilder.makeZeroLiteral(value.getType()));
    // The window is in the lower filter, where it ranges over the same rows after the merge.
    LogicalFilter filtered =
        LogicalFilter.create(LogicalFilter.create(scan, volatilePredicate), selected);
    RelNode modification = original.copy(original.getTraitSet(), List.of(filtered));

    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(modification, provider));
    assertEquals("UPDATE cannot merge nondeterministic filters", error.getMessage());
  }

  private RexNode wrapInScalarSubqueries(RelNode input, RexNode value, int depth) {
    for (int i = 0; i < depth; i++) {
      value =
          RexSubQuery.scalar(
              LogicalProject.create(
                  LogicalValues.createOneRow(input.getCluster()),
                  List.of(),
                  List.of(value),
                  List.of("value")));
    }
    return value;
  }

  private RexNode randomPredicate(RelNode input, int subqueryDepth) {
    RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    return rexBuilder.makeCall(
        SqlStdOperatorTable.LESS_THAN,
        wrapInScalarSubqueries(input, rexBuilder.makeCall(SqlStdOperatorTable.RAND), subqueryDepth),
        rexBuilder.makeApproxLiteral(new BigDecimal("0.5")));
  }

  private ConverterProvider randomProvider() {
    SimpleExtension.ScalarFunctionVariant random =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .name("random")
            .urn("extension:test:random")
            .returnType(TypeCreator.REQUIRED.FP64)
            .build();
    CallConverter randomConverter =
        (call, nested) ->
            call.getOperator() == SqlStdOperatorTable.RAND
                ? Optional.of(
                    ExpressionCreator.scalarFunction(random, TypeCreator.REQUIRED.FP64, List.of()))
                : Optional.empty();
    return ConverterProvider.builder()
        .callConverters(
            converters -> {
              converters.add(0, randomConverter);
              return converters;
            })
        .build();
  }

  private TableModify modification(String sql) throws SqlParseException {
    return assertInstanceOf(
        TableModify.class, SubstraitSqlToCalcite.convertQuery(sql, catalog, provider).rel);
  }

  private Rel convert(String sql) throws SqlParseException {
    return new SqlToSubstrait(provider).convert(sql, catalog).getRoots().get(0).getInput();
  }
}
