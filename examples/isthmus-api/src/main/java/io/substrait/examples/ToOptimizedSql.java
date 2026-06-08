package io.substrait.examples;

import io.substrait.examples.IsthmusAppExamples.Action;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlDialect;

/**
 * Substrait to SQL conversions using Calcite Optimization.
 *
 * <p>This example shows how to convert a Substrait plan to an optimized SQL string. Isthmus handles
 * the Substrait-to-Calcite conversion, after which optimization and SQL generation are standard
 * Calcite operations performed by the caller.
 */
public class ToOptimizedSql implements Action {

  private static List<RelOptRule> simplificationRules() {
    return List.of(CoreRules.FILTER_INTO_JOIN, CoreRules.FILTER_PROJECT_TRANSPOSE);
  }

  @Override
  public void run(String[] args) {
    try {
      System.out.println("Reading from " + args[0]);
      final byte[] buffer = Files.readAllBytes(Paths.get(args[0]));

      final io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(buffer);
      final Plan substraitPlan = new ProtoPlanConverter().from(proto);

      // Configure Isthmus Utilities
      final SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(new ConverterProvider());

      // Configure Calcite Utilities
      final SqlDialect sqlDialect = SqlDialect.DatabaseProduct.MYSQL.getDialect();
      final RelToSqlConverter relToSql = new RelToSqlConverter(sqlDialect);
      final HepProgramBuilder programBuilder = new HepProgramBuilder();
      programBuilder
          .addMatchOrder(HepMatchOrder.BOTTOM_UP)
          .addRuleCollection(simplificationRules());
      final HepPlanner planner = new HepPlanner(programBuilder.build());

      // Convert Substrait to SQL
      for (Plan.Root root : substraitPlan.getRoots()) {
        // Convert Substrait Plan Root to Calcite RelRoot
        final RelRoot relRoot = substraitToCalcite.convert(root);

        // Optimize Calcite RelRoot using HepPlanner
        planner.setRoot(relRoot.project());
        final RelRoot optimized = relRoot.withRel(planner.findBestExp());

        // Generate SQL using Calcite RelToSqlConverter
        final String sql =
            relToSql
                .visitRoot(optimized.project(true))
                .asStatement()
                .toSqlString(sqlDialect)
                .getSql();
        System.out.println(sql);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
