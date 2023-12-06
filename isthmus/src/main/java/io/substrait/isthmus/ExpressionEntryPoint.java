package io.substrait.isthmus;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.ExtendedExpression;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import picocli.CommandLine;

@CommandLine.Command(
    name = "isthmus",
    version = "isthmus 0.1",
    description = "Converts a SQL Expression to a Substrait Extended Expression")
public class ExpressionEntryPoint implements Callable<Integer> {

  @CommandLine.Parameters(index = "0", description = "The sql expression we should parse.")
  private String sqlExpression;

  @CommandLine.Option(
      names = {"-c", "--create"},
      description =
          "One or multiple create table statements e.g. CREATE TABLE T1(foo int, bar bigint)")
  private List<String> createStatements;

  @CommandLine.Option(
      names = {"-m", "--multistatement"},
      description = "Allow multiple statements terminated with a semicolon")
  private boolean allowMultiStatement;

  @CommandLine.Option(
      names = {"--sqlconformancemode"},
      description = "One of built-in Calcite SQL compatibility modes: ${COMPLETION-CANDIDATES}")
  private SqlConformanceEnum sqlConformanceMode = SqlConformanceEnum.DEFAULT;

  // this example implements Callable, so parsing, error handling and handling user
  // requests for usage help or version help can be done with one line of code.
  public static void main(String... args) {
    int exitCode = new CommandLine(new PlanEntryPoint()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    FeatureBoard featureBoard = buildFeatureBoard();
    SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait(featureBoard);
    ExtendedExpression extendedExpression =
        converter.executeSQLExpression(sqlExpression, createStatements);
    System.out.println(
        JsonFormat.printer().includingDefaultValueFields().print(extendedExpression));
    return 0;
  }

  @VisibleForTesting
  FeatureBoard buildFeatureBoard() {
    return ImmutableFeatureBoard.builder()
        .allowsSqlBatch(allowMultiStatement)
        .sqlConformanceMode(sqlConformanceMode)
        .build();
  }
}
