package io.substrait.isthmus.cli;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.FeatureBoard;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SqlExpressionToSubstrait;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.prepare.Prepare;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/** Isthmus CLI entry point. */
@Command(
    name = "isthmus",
    versionProvider = io.substrait.isthmus.cli.IsthmusCliVersion.class,
    description = "Convert SQL Queries and SQL Expressions to Substrait",
    mixinStandardHelpOptions = true)
public class IsthmusEntryPoint implements Callable<Integer> {
  @Parameters(index = "0", arity = "0..1", description = "A SQL query")
  private String sql;

  @Option(
      names = {"-e", "--expression"},
      arity = "1..*",
      description = "One or more SQL expressions e.g. col + 1")
  private String[] sqlExpressions;

  @Option(
      names = {"-c", "--create"},
      description =
          "One or multiple create table statements e.g. CREATE TABLE T1(foo int, bar bigint)")
  private List<String> createStatements = List.of();

  @Option(
      names = {"--outputformat"},
      defaultValue = "PROTOJSON",
      description = "Set the output format for the generated plan: ${COMPLETION-CANDIDATES}")
  private OutputFormat outputFormat = OutputFormat.PROTOJSON;

  enum OutputFormat {
    PROTOJSON, // protobuf json format
    PROTOTEXT, // protobuf text format
    BINARY, // protobuf BINARY format
  }

  @Option(
      names = {"--unquotedcasing"},
      description = "Calcite's casing policy for unquoted identifiers: ${COMPLETION-CANDIDATES}")
  private Casing unquotedCasing = Casing.TO_UPPER;

  /**
   * Standard Java Main method.
   *
   * @param args Isthmus CLI arguments.
   */
  public static void main(String... args) {
    CommandLine commandLine = new CommandLine(new IsthmusEntryPoint());
    commandLine.setCaseInsensitiveEnumValuesAllowed(true);
    CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
    if (parseResult.originalArgs().isEmpty()) { // If no arguments print usage help
      commandLine.usage(System.out);
      System.exit(0);
    }
    if (commandLine.isUsageHelpRequested()) {
      commandLine.usage(System.out);
      System.exit(0);
    }
    if (commandLine.isVersionHelpRequested()) {
      commandLine.printVersionHelp(System.out);
      System.exit(0);
    }
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    FeatureBoard featureBoard = buildFeatureBoard();
    // Isthmus image is parsing SQL Expression if that argument is defined
    if (sqlExpressions != null) {
      SqlExpressionToSubstrait converter =
          new SqlExpressionToSubstrait(featureBoard, DefaultExtensionCatalog.DEFAULT_COLLECTION);
      ExtendedExpression extendedExpression = converter.convert(sqlExpressions, createStatements);
      printMessage(extendedExpression);
    } else { // by default Isthmus image are parsing SQL Query
      SqlToSubstrait converter = new SqlToSubstrait(featureBoard);
      Prepare.CatalogReader catalog =
          SubstraitCreateStatementParser.processCreateStatementsToCatalog(
              createStatements.toArray(String[]::new));
      Plan plan = new PlanProtoConverter().toProto(converter.convert(sql, catalog));
      printMessage(plan);
    }
    return 0;
  }

  private void printMessage(Message message) throws IOException {
    if (outputFormat == OutputFormat.PROTOJSON) {
      System.out.println(JsonFormat.printer().includingDefaultValueFields().print(message));
    } else if (outputFormat == OutputFormat.PROTOTEXT) {
      TextFormat.printer().print(message, System.out);
    } else if (outputFormat == OutputFormat.BINARY) {
      message.writeTo(System.out);
    }
  }

  @VisibleForTesting
  FeatureBoard buildFeatureBoard() {
    return ImmutableFeatureBoard.builder().unquotedCasing(unquotedCasing).build();
  }
}
