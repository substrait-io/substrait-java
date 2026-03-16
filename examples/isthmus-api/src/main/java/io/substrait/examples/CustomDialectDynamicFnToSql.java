package io.substrait.examples;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.examples.IsthmusAppExamples.Action;
import io.substrait.expression.Expression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.DynamicConverterProvider;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.plan.Plan;
import io.substrait.relation.Filter;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.SparkSqlDialect;

/**
 * Example use of dynamic functions and the {@link SubstraitBuilder} API.
 *
 * <p>SparkSQL supports regular rexpression matching with `RLIKE`
 *
 * <p>This example creates a dynamic function that enables plans to be create that use the
 * `regexp_matches` function. A custom SQL dialect can map that to operations supported by SparkSQL.
 *
 * <p>./gradlew examples:isthmus-api:run --args "CustomDialectDynamicFnToSql"
 */
public class CustomDialectDynamicFnToSql implements Action {

  @Override
  public void run(final String[] args) {

    // Load the example functions
    final String exampleCustonFunctionPath = "/extensions/example_scalar_functions_custom.yaml";

    // ensure these are merged with the default collection
    final SimpleExtension.ExtensionCollection customExtensions =
        SimpleExtension.load(List.of(exampleCustonFunctionPath));

    final SimpleExtension.ExtensionCollection extensions =
        DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(customExtensions);

    // Create a Substrait builder with default extensions
    final SubstraitBuilder builder = new io.substrait.dsl.SubstraitBuilder(extensions);

    // Setup a list of the names of each of the columns
    // start with the overall record count
    final List<String> names = List.of("id", "colour");
    // Create a named scan for a table with columns: id (i32), colour (string)
    final Rel scan =
        builder.namedScan(
            List.of("my_table"),
            List.of("id", "colour"),
            List.of(TypeCreator.REQUIRED.I32, TypeCreator.REQUIRED.varChar(255)));

    final List<Expression> fnArgs =
        List.of(
            builder.fieldReference(scan, 1), // colour is at index 1
            builder.str("green"));

    final Expression fn =
        builder.scalarFn(
            "extension:substrait:functions_custom",
            "regexp_matches:vchar_str",
            TypeCreator.REQUIRED.BOOLEAN,
            fnArgs.toArray(new Expression[0]));

    // Create a filter: colour == 'green'
    final Rel filter = Filter.builder().input(scan).condition(fn).remap(Optional.empty()).build();

    // Use filter as the root relation (no project needed for simple selection)
    final Plan plan = builder.plan(Plan.Root.builder().input(filter).names(names).build());

    System.out.println("\nCreated the Substrait plan::");
    System.out.println(plan);

    // Convert the plan to SQL
    final SubstraitToSql substraitToSql =
        new SubstraitToSql(new DynamicConverterProvider(extensions));

    System.out.println("\nWith custom SparkSQL SqlDialect::");
    substraitToSql.convert(plan, customSqlDialect()).stream().forEachOrdered(System.out::println);
  }

  /** Create a Custom dialect. Converts the function 'regexp_matches' to 'RLIKE' */
  public static final SqlDialect customSqlDialect() {

    return new SparkSqlDialect(SparkSqlDialect.DEFAULT_CONTEXT) {

      @Override
      public void unparseCall(
          final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        if ("REGEXP_MATCHES".equalsIgnoreCase(call.getOperator().getName())) {
          // Convert REGEXP_MATCHES(aa, bb) to a RLIKE
          if (call.operandCount() == 2) {
            call.operand(0).unparse(writer, leftPrec, rightPrec);
            writer.keyword("RLIKE");
            call.operand(1).unparse(writer, leftPrec, rightPrec);
          } else {
            // Fallback to default if operand count is unexpected
            super.unparseCall(writer, call, leftPrec, rightPrec);
          }
        } else {
          super.unparseCall(writer, call, leftPrec, rightPrec);
        }
      }
    };
  }
}
