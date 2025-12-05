package io.substrait.examples;

import io.substrait.examples.IsthmusAppExamples.Action;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.Root;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

/**
 * Substrait to SQL conversions.
 *
 * <p>The conversion process involves three steps:
 *
 * <p>1. Load the plan into the protobuf object and create an in-memory POJO representation.
 *
 * <p>2. Create a Converter to map the Substrait plan to Calcite relations. This requires the type
 * system to use and the collection of extensions from the substrait plan.
 *
 * <p>3. Convert the Calcite relational nodes to SQL statements using the specified SQL dialect
 * configuration.
 *
 * <p>It is possible to get multiple SQL statements from a single Substrait plan.
 */
public class ToSql implements Action {

  @Override
  public void run(String[] args) {

    try {

      // Load the protobuf binary file into a Substrait Plan POJO
      System.out.println("Reading from " + args[0]);
      final byte[] buffer = Files.readAllBytes(Paths.get(args[0]));

      final io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(buffer);
      final ProtoPlanConverter protoToPlan = new ProtoPlanConverter();
      final Plan substraitPlan = protoToPlan.from(proto);

      // Create the proto plan to display to stdout - as it has a better format
      final PlanProtoConverter planToProto = new PlanProtoConverter();
      final io.substrait.proto.Plan protoPlan = planToProto.toProto(substraitPlan);
      System.out.println(protoPlan);

      final SimpleExtension.ExtensionCollection extensions =
          DefaultExtensionCatalog.DEFAULT_COLLECTION;
      final SubstraitToCalcite converter =
          new SubstraitToCalcite(
              extensions, new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM));

      // Determine which SQL Dialect we want the converted queries to be in
      final SqlDialect sqlDialect = SqlDialect.DatabaseProduct.MYSQL.getDialect();

      // Create the Sql to Calcite Relation Parser
      final RelToSqlConverter relToSql = new RelToSqlConverter(sqlDialect);
      final List<String> sqlStrings = new ArrayList<>();

      System.out.println("\n");
      // and get each root from the Substrait plan
      for (final Root root : substraitPlan.getRoots()) {
        // Substrait -> Calcite
        final RelNode calciteRelNode = converter.convert(root).project(true);
        // Calcite -> SQL
        final SqlNode sqlNode = relToSql.visitRoot(calciteRelNode).asStatement();

        final String sqlString = sqlNode.toSqlString(sqlDialect).getSql();
        sqlStrings.add(sqlString);
      }
      sqlStrings.forEach(System.out::println);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
