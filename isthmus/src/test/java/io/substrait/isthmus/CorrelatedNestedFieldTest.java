package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class CorrelatedNestedFieldTest {

  @Test
  void nestedOuterFieldKeepsItsCorrelationAnchor() throws SqlParseException {
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE outer_table (id INTEGER NOT NULL, s ROW(v INTEGER NOT NULL) NOT NULL);"
                + "CREATE TABLE inner_table (id INTEGER NOT NULL, s ROW(v INTEGER NOT NULL) NOT NULL)");
    Plan plan =
        new PlanProtoConverter()
            .toProto(
                new SqlToSubstrait()
                    .convert(
                        "SELECT o.id FROM outer_table o WHERE EXISTS"
                            + " (SELECT 1 FROM inner_table i WHERE i.id = o.s.v)",
                        catalog));

    FilterRel outerFilter =
        plan.getRelations(0).getRoot().getInput().getProject().getInput().getFilter();
    FilterRel innerFilter =
        outerFilter.getCondition().getSubquery().getSetPredicate().getTuples().getFilter();
    Expression.FieldReference outerField =
        innerFilter.getCondition().getScalarFunction().getArguments(1).getValue().getSelection();

    assertTrue(outerFilter.getInput().getRead().getCommon().hasRelAnchor());
    assertTrue(outerField.hasOuterReference());
    assertTrue(outerField.getOuterReference().hasRelReference());
    assertEquals(
        outerFilter.getInput().getRead().getCommon().getRelAnchor(),
        outerField.getOuterReference().getRelReference());
    assertEquals(1, outerField.getDirectReference().getStructField().getField());
    assertTrue(outerField.getDirectReference().getStructField().hasChild());
    assertEquals(
        0, outerField.getDirectReference().getStructField().getChild().getStructField().getField());
    assertTrue(
        innerFilter
            .getCondition()
            .getScalarFunction()
            .getArguments(0)
            .getValue()
            .getSelection()
            .hasRootReference());
  }
}
