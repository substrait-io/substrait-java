package io.substrait.examples;

import io.substrait.examples.IsthmusAppExamples.Action;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.calcite.sql.SqlDialect;

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

      // Determine which SQL Dialect we want the converted queries to be in
      final SqlDialect sqlDialect = SqlDialect.DatabaseProduct.MYSQL.getDialect();

      SubstraitToSql substraitToSql = new SubstraitToSql();

      System.out.println("\n");

      // Convert each of the Substrait plan roots to SQL
      substraitToSql.convert(substraitPlan, sqlDialect).stream()
          .forEachOrdered(System.out::println);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
