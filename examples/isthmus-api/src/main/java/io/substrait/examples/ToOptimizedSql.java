package io.substrait.examples;

import io.substrait.examples.IsthmusAppExamples.Action;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.Root;
import io.substrait.plan.ProtoPlanConverter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlDialect;

/**
 * Substrait to SQL conversions using Calcite Optimization.
 *
 * <p>This example follows the same structure as the "ToSql" example but shows how the
 * ConverterProvider can be subclassed to update the Calcite configuration.
 *
 * <p>This case how the HepPlanner can be used to optimize the plan before it's conversion to SQL.
 */
public class ToOptimizedSql implements Action {

  /**
   * Custom ConverterProvider.
   *
   * <p>Specifically overrides the SubstraitToCalcite to allow the plan to be optimised
   */
  static final class OptimizingConverterProvider extends ConverterProvider {

    /**
     * Set of calcite rules to use.
     *
     * <p>Can be configured as you wish.
     *
     * @return List of rules
     */
    public static List<RelOptRule> simplificationRules() {
      return List.of(CoreRules.FILTER_INTO_JOIN, CoreRules.FILTER_PROJECT_TRANSPOSE);
    }

    /** Returns a subclass of the SubstraitToCalcite class. */
    @Override
    protected SubstraitToCalcite getSubstraitToCalcite() {

      return new SubstraitToCalcite(this) {

        @Override
        public RelRoot convert(Root root) {
          final HepProgramBuilder programBuilder = new HepProgramBuilder();
          programBuilder
              .addMatchOrder(HepMatchOrder.BOTTOM_UP)
              .addRuleCollection(simplificationRules());

          final RelOptPlanner hepPlanner = new HepPlanner(programBuilder.build());
          // convert the substrait to the calcite relation tree
          final RelRoot convertedRoot = super.convert(root);
          hepPlanner.setRoot(convertedRoot.project());

          // and then call the optimizer and return the result
          return convertedRoot.withRel(hepPlanner.findBestExp());
        }
      };
    }
  }

  @Override
  public void run(String[] args) {

    try {

      // Load the protobuf binary file into a Substrait Plan POJO
      System.out.println("Reading from " + args[0]);
      final byte[] buffer = Files.readAllBytes(Paths.get(args[0]));

      final io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(buffer);
      final ProtoPlanConverter protoToPlan = new ProtoPlanConverter();
      final Plan substraitPlan = protoToPlan.from(proto);

      // Determine which SQL Dialect we want the converted queries to be in
      final SqlDialect sqlDialect = SqlDialect.DatabaseProduct.MYSQL.getDialect();

      // Use a custom ConverterProvider
      final SubstraitToSql substraitToSql = new SubstraitToSql(new OptimizingConverterProvider());

      // Convert each of the Substrait plan roots to SQL
      substraitToSql.convert(substraitPlan, sqlDialect).stream()
          .forEachOrdered(System.out::println);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
