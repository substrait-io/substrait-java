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
import org.apache.calcite.sql.SqlDialect;

/**
 * Example use of dynamic functions and the {@link SubstraitBuilder} API.
 *
 * <p>DuckDB used here supports the function `regexp_matches' for comparing values with a regular
 * expression.
 *
 * <p>This example creates a dynamic function that enables plans to be create that use the
 * `regexp_matches` function.
 *
 * <p>./gradlew examples:isthmus-api:run --args "DynamicFnToSql"
 */
public class DynamicFnToSql implements Action {

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
    System.out.println("\nWith default DuckDB SqlDialect::");
    substraitToSql.convert(plan, SqlDialect.DatabaseProduct.DUCKDB.getDialect()).stream()
        .forEachOrdered(System.out::println);
  }
}
