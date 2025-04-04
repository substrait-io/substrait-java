package io.substrait.isthmus.cli;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.FeatureBoard;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class IsthmusEntryPointTest {

  /** Test that the default values are set correctly into the {@link FeatureBoard}. */
  @Test
  void defaultFeatureBoard() {
    IsthmusEntryPoint isthmusEntryPoint = new IsthmusEntryPoint();
    new CommandLine(isthmusEntryPoint);
    FeatureBoard features = isthmusEntryPoint.buildFeatureBoard();
    assertFalse(features.allowsSqlBatch());
  }

  /** Test that the command line options are correctly parsed into the {@link FeatureBoard}. */
  @Test
  void customFeatureBoard() {
    IsthmusEntryPoint isthmusEntryPoint = new IsthmusEntryPoint();
    new CommandLine(isthmusEntryPoint).parseArgs("--multistatement", "SELECT * FROM foo");
    FeatureBoard features = isthmusEntryPoint.buildFeatureBoard();
    assertTrue(features.allowsSqlBatch());
  }
}
