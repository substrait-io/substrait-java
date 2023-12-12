package io.substrait.isthmus;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.JsonFormat;
import io.substrait.isthmus.SubstraitRelVisitor.CrossJoinPolicy;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import picocli.CommandLine;

@Command(
    name = "isthmus",
    version = "isthmus 0.1",
    description = "Substrait Java Native Image for parsing SQL Query and SQL Expressions")
public class IsthmusEntryPoint implements Callable<Integer> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IsthmusEntryPoint.class);

  @Parameters(index = "0", arity = "0..1", description = "The sql we should parse.")
  private String sql;

  @Option(
      names = {"-e", "--sqlExpression"},
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
      names = {"--sqlconformancemode"},
      description = "One of built-in Calcite SQL compatibility modes: ${COMPLETION-CANDIDATES}")
  private SqlConformanceEnum sqlConformanceMode = SqlConformanceEnum.DEFAULT;

  @Option(
      names = {"--crossjoinpolicy"},
      description = "One of built-in Calcite SQL compatibility modes: ${COMPLETION-CANDIDATES}")
  private CrossJoinPolicy crossJoinPolicy = CrossJoinPolicy.KEEP_AS_CROSS_JOIN;

  // this example implements Callable, so parsing, error handling and handling user
  // requests for usage help or version help can be done with one line of code.
  public static void main(String... args) {
    logger.debug(Arrays.toString(args));
    int exitCode = new CommandLine(new IsthmusEntryPoint()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    FeatureBoard featureBoard = buildFeatureBoard();
    // Isthmus image is paring SQL Expression if that argument is defined
    if (sqlExpression != null) {
      logger.debug(sqlExpression);
      logger.debug(String.valueOf(createStatements));
      SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait(featureBoard);
      ExtendedExpression extendedExpression =
          converter.executeSQLExpression(sqlExpression, createStatements);
      System.out.println(
          JsonFormat.printer().includingDefaultValueFields().print(extendedExpression));
    } else { // by default Isthmus image are parsing SQL Query
      logger.debug(sql);
      logger.debug(String.valueOf(createStatements));
      SqlToSubstrait converter = new SqlToSubstrait(featureBoard);
      Plan plan = converter.execute(sql, createStatements);
      System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
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
