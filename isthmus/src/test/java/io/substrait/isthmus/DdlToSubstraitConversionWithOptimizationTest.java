package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.isthmus.calcite.rel.CreateTable;
import io.substrait.isthmus.calcite.rel.CreateView;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.Plan;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedWrite;
import java.util.List;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class that verifies Substrait conversion works correctly with Calcite optimization rules
 * applied to DDL statements.
 *
 * <p>This ensures that optimized DDL plans (which involve Calcite calling {@code copy} or {@code
 * replaceInput} on the DDL relational nodes) can be successfully converted to Substrait format
 * without throwing assertions or losing state.
 */
class DdlToSubstraitConversionWithOptimizationTest {

  /** Parser configuration for DDL statements. */
  private static final SqlParser.Config DDL_PARSER_CONFIG =
      SqlDialect.DatabaseProduct.CALCITE
          .getDialect()
          .configureParser(SqlParser.config().withParserFactory(ServerDdlExecutor.PARSER_FACTORY));

  private final ConverterProvider converterProvider = new ConverterProvider();

  /**
   * Tests that optimization rules can be safely applied to DDL statements.
   *
   * <p>Because Calcite's rule engine triggers {@code copy} and {@code replaceInput} during
   * optimization, a single transformation rule is sufficient to prove that the parent DDL wrapper
   * node safely handles child mutations.
   */
  @ParameterizedTest
  @ValueSource(strings = {"create table", "create view"})
  void testDdlOptimizationMutation(String ddlType) throws SqlParseException {
    final String ddl =
        "create table src1 (intcol int, charcol varchar(10));"
            + "create table src2 (id int, name varchar(20), amount int)";

    Prepare.CatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(ddl);

    String createStatement =
        String.format(
            "%s dst1 as SELECT * FROM src1 JOIN src2 ON src1.intcol = src2.id WHERE src2.amount > 100",
            ddlType);

    // Convert DDL to RelRoot
    List<RelRoot> relRoots =
        SubstraitSqlToCalcite.convertQueries(createStatement, catalogReader, DDL_PARSER_CONFIG);
    RelRoot relRoot = relRoots.get(0);

    // Apply an optimization rule (FILTER_INTO_JOIN) that will structurally mutate the children
    // to prove the DDL wrapper handles the replacement without failure.
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);

    HepPlanner planner = new HepPlanner(builder.build());
    planner.setRoot(relRoot.rel);
    RelNode optimizedPlan = planner.findBestExp();

    // 1. Verify that `copy` properly propagated the RelTraitSet
    assertEquals(
        relRoot.rel.getTraitSet(),
        optimizedPlan.getTraitSet(),
        "Optimization should preserve the wrapper's original trait set");

    // 2. Verify the underlying rel actually got optimized (Filter moved into Join condition)
    org.apache.calcite.rel.core.Project project =
        assertInstanceOf(
            org.apache.calcite.rel.core.Project.class,
            optimizedPlan.getInput(0),
            "Underlying query should have a top-level Project");

    assertInstanceOf(
        Join.class, project.getInput(), "Filter should be pushed into the Join under the Project");

    // 3. Verify DDL wrapper integrity is preserved
    boolean isTable = "create table".equals(ddlType);
    if (isTable) {
      CreateTable ct = assertInstanceOf(CreateTable.class, optimizedPlan);
      assertEquals("DST1", ct.getTableName().get(0));
    } else {
      CreateView cv = assertInstanceOf(CreateView.class, optimizedPlan);
      assertEquals("DST1", cv.getViewName().get(0));
    }

    RelRoot optimizedRelRoot = RelRoot.of(optimizedPlan, relRoot.kind);

    // 4. Convert optimized plan to Substrait and deeply verify the Substrait structures
    Plan.Root substraitRoot = SubstraitRelVisitor.convert(optimizedRelRoot, converterProvider);

    assertNotNull(substraitRoot, "Substrait root should not be null");
    assertNotNull(substraitRoot.getInput(), "Substrait root input should not be null");

    if (isTable) {
      NamedWrite write = assertInstanceOf(NamedWrite.class, substraitRoot.getInput());
      assertEquals("DST1", write.getNames().get(0));
    } else {
      NamedDdl namedDdl = assertInstanceOf(NamedDdl.class, substraitRoot.getInput());
      assertEquals("DST1", namedDdl.getNames().get(0));
    }
  }
}
