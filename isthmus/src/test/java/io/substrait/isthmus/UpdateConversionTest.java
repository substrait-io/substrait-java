package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
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

  private TableModify modification(String sql) throws SqlParseException {
    return assertInstanceOf(
        TableModify.class, SubstraitSqlToCalcite.convertQuery(sql, catalog, provider).rel);
  }

  private Rel convert(String sql) throws SqlParseException {
    return new SqlToSubstrait(provider).convert(sql, catalog).getRoots().get(0).getInput();
  }
}
