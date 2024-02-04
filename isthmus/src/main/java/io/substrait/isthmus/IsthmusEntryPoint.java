package io.substrait.isthmus;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelVisitor.CrossJoinPolicy;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import picocli.CommandLine;

@Command(
    name = "isthmus",
    version = "isthmus 0.1",
    description = "Substrait Java Native Image for parsing SQL Query and SQL Expressions",
    mixinStandardHelpOptions = true)
public class IsthmusEntryPoint implements Callable<Integer> {
  @Parameters(index = "0", arity = "0..1", description = "The sql we should parse.")
  private String sql;

  @Option(
      names = {"-e", "--expression"},
      description = "The sql expression we should parse.")
  private String sqlExpression;

  @Option(
      names = {"-c", "--create"},
      description =
          "One or multiple create table statements e.g. CREATE TABLE T1(foo int, bar bigint)")
  private List<String> createStatements;

  @Option(
      names = {"-m", "--multistatement"},
      description = "Allow multiple statements terminated with a semicolon")
  private boolean allowMultiStatement;

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
      names = {"--sqlconformancemode"},
      description = "One of built-in Calcite SQL compatibility modes: ${COMPLETION-CANDIDATES}")
  private SqlConformanceEnum sqlConformanceMode = SqlConformanceEnum.DEFAULT;

  @Option(
      names = {"--crossjoinpolicy"},
      description = "One of built-in Calcite SQL compatibility modes: ${COMPLETION-CANDIDATES}")
  private CrossJoinPolicy crossJoinPolicy = CrossJoinPolicy.KEEP_AS_CROSS_JOIN;

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
    if (sqlExpression != null) {
      SqlExpressionToSubstrait converter =
          new SqlExpressionToSubstrait(featureBoard, SimpleExtension.loadDefaults());
      ExtendedExpression extendedExpression = converter.convert(sqlExpression, createStatements);
      switch (outputFormat) {
        case PROTOJSON -> System.out.println(
            JsonFormat.printer().includingDefaultValueFields().print(extendedExpression));
        case PROTOTEXT -> TextFormat.printer().print(extendedExpression, System.out);
        case BINARY -> extendedExpression.writeTo(System.out);
      }
    } else { // by default Isthmus image are parsing SQL Query
      SqlToSubstrait converter = new SqlToSubstrait(featureBoard);
      Plan plan = converter.execute(sql, createStatements);
      switch (outputFormat) {
        case PROTOJSON -> System.out.println(
            JsonFormat.printer().includingDefaultValueFields().print(plan));
        case PROTOTEXT -> TextFormat.printer().print(plan, System.out);
        case BINARY -> plan.writeTo(System.out);
      }
    }
    return 0;
  }

  @VisibleForTesting
  FeatureBoard buildFeatureBoard() {
    return ImmutableFeatureBoard.builder()
        .allowsSqlBatch(allowMultiStatement)
        .sqlConformanceMode(sqlConformanceMode)
        .crossJoinPolicy(crossJoinPolicy)
        .build();
  }
}
