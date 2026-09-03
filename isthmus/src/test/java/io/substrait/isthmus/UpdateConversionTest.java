package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UpdateConversionTest {
  private final ConverterProvider provider = ConverterProvider.DEFAULT;
  private final Prepare.CatalogReader catalog =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog(
          provider, "CREATE TABLE src1 (intcol INT, charcol VARCHAR(10))");

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
            convert("SELECT (intcol + 1) + 2 FROM src1 WHERE intcol + 1 > 10 AND charcol = 'a'"));

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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "UPDATE src1 SET intcol = 10 WHERE EXISTS"
            + " (SELECT 1 FROM src1 AS other WHERE other.intcol = src1.intcol)",
        "UPDATE src1 SET intcol ="
            + " (SELECT MAX(other.intcol) FROM src1 AS other WHERE other.charcol = src1.charcol)"
      })
  void rejectsRemovedCorrelationBindings(String sql) {
    assertThrows(UnsupportedOperationException.class, () -> convert(sql));
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

    assertThrows(
        UnsupportedOperationException.class,
        () -> SubstraitRelVisitor.convert(modification, provider));
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
            rexBuilder.makeInputRef(projected, 2),
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
