package io.substrait.isthmus.cli;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import io.substrait.isthmus.ConverterProvider;
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
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

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

  @Option(
      names = {"--stacktrace"},
      description = "Print the full stack trace of a conversion failure, not just its message")
  private boolean stackTrace;

  @Spec private CommandSpec spec;

  /**
   * Standard Java Main method invoked by the isthmus CLI command.
   *
   * @param args Isthmus CLI arguments.
   */
  public static void main(String... args) {
    System.exit(createCommandLine().execute(args));
  }

  /**
   * Creates the {@link CommandLine} driving the isthmus CLI. Errors caused by the given SQL are
   * reported by {@link IsthmusExecutionExceptionHandler} rather than as a stack trace.
   *
   * @return the configured {@link CommandLine}
   */
  static CommandLine createCommandLine() {
    CommandLine commandLine = new CommandLine(new IsthmusEntryPoint());
    commandLine.setCaseInsensitiveEnumValuesAllowed(true);
    commandLine.setExecutionExceptionHandler(new IsthmusExecutionExceptionHandler());
    return commandLine;
  }

  @Override
  public Integer call() throws Exception {
    if (sqlExpressions == null && sql == null) {
      throw new ParameterException(
          spec.commandLine(),
          "Missing SQL to convert: pass a SQL query as the first argument, "
              + "or SQL expressions with -e / --expression");
    }
    if (sqlExpressions != null && sql != null) {
      throw new ParameterException(
          spec.commandLine(),
          "Give either a SQL query or -e / --expression, not both: the query '"
              + sql
              + "' would be ignored");
    }
    ConverterProvider provider = ConverterProvider.builder().unquotedCasing(unquotedCasing).build();
    // Isthmus image is parsing SQL Expression if that argument is defined
    if (sqlExpressions != null) {
      SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait(provider);
      ExtendedExpression extendedExpression = converter.convert(sqlExpressions, createStatements);
      printMessage(extendedExpression);
    } else { // by default Isthmus image are parsing SQL Query
      SqlToSubstrait converter = new SqlToSubstrait(provider);
      Prepare.CatalogReader catalog =
          SubstraitCreateStatementParser.processCreateStatementsToCatalog(
              provider, createStatements);
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
}
