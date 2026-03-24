package io.substrait.examples;

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

import io.substrait.examples.IsthmusAppExamples.Action;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.Root;
import io.substrait.plan.ProtoPlanConverter;

/**
 * Substrait to SQL conversions.
 *
 * <p>
 * The conversion process involves three steps:
 *
 * <p>
 * 1. Load the plan into the protobuf object and create an in-memory POJO
 * representation.
 *
 * <p>
 * 2. Create a Converter to map the Substrait plan to Calcite relations. This
 * requires the type
 * system to use and the collection of extensions from the substrait plan.
 *
 * <p>
 * 3. Convert the Calcite relational nodes to SQL statements using the specified
 * SQL dialect
 * configuration.
 *
 * <p>
 * It is possible to get multiple SQL statements from a single Substrait plan.
 */
public class ToOptimizedSql implements Action {

  static final class OptimizingConverterProvider extends ConverterProvider {
    public static List<RelOptRule> simplificationRules() {
      return List.of(
          CoreRules.FILTER_INTO_JOIN, CoreRules.FILTER_PROJECT_TRANSPOSE);
    }

    @Override
    protected SubstraitToCalcite getSubstraitToCalcite() {

      return new SubstraitToCalcite(this) {

        @Override
        public RelRoot convert(Root root) {
          final HepProgramBuilder programBuilder = new HepProgramBuilder();
          // For safety, in case we land in a loop
          programBuilder.addMatchLimit(5000);
          programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP)
              .addRuleCollection(simplificationRules());
          final RelOptPlanner hepPlanner = new HepPlanner(programBuilder.build());

          final RelRoot convertedRoot = super.convert(root);
          hepPlanner.setRoot(convertedRoot.project());

          System.out.println("Optimizing the output");
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

      final SubstraitToSql substraitToSql = new SubstraitToSql(new OptimizingConverterProvider());

      // Convert each of the Substrait plan roots to SQL
      substraitToSql.convert(substraitPlan, sqlDialect).stream()
          .forEachOrdered(System.out::println);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
