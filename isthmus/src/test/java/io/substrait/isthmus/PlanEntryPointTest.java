package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.SubstraitRelVisitor.CrossJoinPolicy;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

class PlanEntryPointTest {

  /** Test that the default values are set correctly into the {@link FeatureBoard}. */
  @Test
  void defaultFeatureBoard() {
    PlanEntryPoint planEntryPoint = new PlanEntryPoint();
    new CommandLine(planEntryPoint);
    FeatureBoard features = planEntryPoint.buildFeatureBoard();
    assertFalse(features.allowsSqlBatch());
    assertEquals(SqlConformanceEnum.DEFAULT, features.sqlConformanceMode());
    assertEquals(CrossJoinPolicy.KEEP_AS_CROSS_JOIN, features.crossJoinPolicy());
  }

  /** Test that the command line options are correctly parsed into the {@link FeatureBoard}. */
  @Test
  void customFeatureBoard() {
    PlanEntryPoint planEntryPoint = new PlanEntryPoint();
    new CommandLine(planEntryPoint)
        .parseArgs(
            "--multistatement",
            "--sqlconformancemode=SQL_SERVER_2008",
            "--crossjoinpolicy=CONVERT_TO_INNER_JOIN",
            "SELECT * FROM foo");
    FeatureBoard features = planEntryPoint.buildFeatureBoard();
    assertTrue(features.allowsSqlBatch());
    assertEquals(SqlConformanceEnum.SQL_SERVER_2008, features.sqlConformanceMode());
    assertEquals(CrossJoinPolicy.CONVERT_TO_INNER_JOIN, features.crossJoinPolicy());
  }

  /**
   * Test that the command line parser throws an exception when an invalid join policy is specified.
   */
  @Test
  void invalidCmdOptions() {
    PlanEntryPoint planEntryPoint = new PlanEntryPoint();
    assertThrows(
        ParameterException.class,
        () ->
            new CommandLine(planEntryPoint)
                .parseArgs(
                    "--sqlconformancemode=SQL_SERVER_2008",
                    "--crossjoinpolicy=REWRITE_TO_INNER_JOIN"));
  }
}
