package io.substrait.isthmus;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.JsonFormat;
import io.substrait.isthmus.SubstraitRelVisitor.CrossJoinPolicy;
import io.substrait.proto.Plan;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import picocli.CommandLine;
import picocli.CommandLine.PropertiesDefaultProvider;

@Command(
    name = "isthmus",
    version = "isthmus 0.1",
    defaultValueProvider = PropertiesDefaultProvider.class,
    description = "Converts a SQL query to a Substrait Plan")
public class PlanEntryPoint implements Callable<Integer> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanEntryPoint.class);

  @Parameters(index = "0", description = "The sql we should parse.")
  private String sql;

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
    int exitCode = new CommandLine(new PlanEntryPoint()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    FeatureBoard featureBoard = buildFeatureBoard();
    SqlToSubstrait converter = new SqlToSubstrait(featureBoard);
    Plan plan = converter.execute(sql, createStatements);
    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
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
