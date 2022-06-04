package io.substrait.isthmus;

import static io.substrait.isthmus.SqlToSubstrait.StatementBatching.MULTI_STATEMENT;
import static io.substrait.isthmus.SqlToSubstrait.StatementBatching.SINGLE_STATEMENT;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.Plan;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@Command(
    name = "isthmus",
    version = "isthmus 0.1",
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

  @Override
  public Integer call() throws Exception {
    SqlToSubstrait converter =
        new SqlToSubstrait(
            new SqlToSubstrait.Options(allowMultiStatement ? MULTI_STATEMENT : SINGLE_STATEMENT));
    Plan plan = converter.execute(sql, createStatements);
    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
    return 0;
  }

  // this example implements Callable, so parsing, error handling and handling user
  // requests for usage help or version help can be done with one line of code.
  public static void main(String... args) {
    int exitCode = new CommandLine(new PlanEntryPoint()).execute(args);
    System.exit(exitCode);
  }
}
