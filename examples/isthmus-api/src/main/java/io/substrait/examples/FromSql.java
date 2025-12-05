package io.substrait.examples;

import io.substrait.examples.IsthmusAppExamples.Action;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * Substrait from SQL conversions.
 *
 * <p>The conversion process involves four steps:
 *
 * <p>1. Create a fully typed schema for the inputs. Within a SQL context this represents the CREATE
 * TABLE commands, which need to be converted to a Calcite Schema.
 *
 * <p>2. Parse the SQL query to convert (in the source SQL dialect).
 *
 * <p>3. Convert the SQL query to Calcite Relations.
 *
 * <p>4. Convert the Calcite Relations to Substrait relations.
 *
 * <p>Note that the schema could be created from other means, such as Calcite's reflection-based
 * schema.
 */
public class FromSql implements Action {

  @Override
  public void run(final String[] args) {
    try {
      final String createSql =
          """
                    CREATE TABLE "vehicles" ("vehicle_id" varchar(15), "make" varchar(40), "model" varchar(40),
                        "colour" varchar(15), "fuel_type" varchar(15),
                        "cylinder_capacity" int, "first_use_date" varchar(15));

                    CREATE TABLE "tests" ("test_id" varchar(15), "vehicle_id" varchar(15),
                             "test_date" varchar(20), "test_class" varchar(20), "test_type" varchar(20),
                             "test_result" varchar(15),"test_mileage" int, "postcode_area" varchar(15));

                      """;

      // Create the Calcite Schema from the CREATE TABLE statements.
      // The Isthmus helper classes assume a standard SQL format for parsing.
      final CalciteSchema calciteSchema = CalciteSchema.createRootSchema(false);
      SubstraitCreateStatementParser.processCreateStatements(createSql)
          .forEach(t -> calciteSchema.add(t.getName(), t));

      // Type Factory based on Java Types
      final RelDataTypeFactory typeFactory =
          new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM);

      // Default configuration for calcite
      final CalciteConnectionConfig calciteDefaultConfig =
          CalciteConnectionConfig.DEFAULT.set(
              CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString());

      final CalciteCatalogReader catalogReader =
          new CalciteCatalogReader(calciteSchema, List.of(), typeFactory, calciteDefaultConfig);

      // Query that needs to be converted; again this could be in a variety of SQL dialects
      final String apacheDerbyQuery =
          """
          SELECT vehicles.colour, count(*) as colourcount FROM vehicles INNER JOIN tests
              ON vehicles.vehicle_id=tests.vehicle_id WHERE tests.test_result = 'P'
              GROUP BY vehicles.colour ORDER BY count(*)
          """;
      final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();

      // choose Apache Derby as an example dialect
      final SqlDialect dialect = SqlDialect.DatabaseProduct.DERBY.getDialect();
      final Plan substraitPlan = sqlToSubstrait.convert(apacheDerbyQuery, catalogReader, dialect);

      // Create the proto plan to display to stdout - as it has a better format
      final PlanProtoConverter planToProto = new PlanProtoConverter();
      final io.substrait.proto.Plan protoPlan = planToProto.toProto(substraitPlan);
      System.out.println(protoPlan);

      // write out to file if given a file name
      // convert to a protobuff byte array and write as binary file
      if (args.length == 1) {

        final byte[] buffer = protoPlan.toByteArray();
        final Path outputFile = Paths.get(args[0]);
        Files.write(outputFile, buffer);
        System.out.println("File written to " + outputFile);
      }

    } catch (SqlParseException | IOException e) {
      e.printStackTrace();
    }
  }
}
