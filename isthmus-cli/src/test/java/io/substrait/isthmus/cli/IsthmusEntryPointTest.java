package io.substrait.isthmus.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class IsthmusEntryPointTest {

  /** The output the CLI wrote to stderr, and the status code it exited with. */
  private static final class Run {
    final int statusCode;
    final String err;

    Run(int statusCode, String err) {
      this.statusCode = statusCode;
      this.err = err;
    }

    /** Asserts that the error was reported as a message rather than as a stack trace. */
    void assertNoStackTrace() {
      assertFalse(err.contains("\tat "), () -> "unexpected stack trace in:\n" + err);
    }

    /**
     * Asserts that stderr contains the given snippets.
     *
     * @param snippets the snippets stderr must contain
     */
    void assertErrContains(String... snippets) {
      for (String snippet : snippets) {
        assertTrue(err.contains(snippet), () -> "expected '" + snippet + "' in:\n" + err);
      }
    }
  }

  /**
   * Runs the CLI with the given arguments, capturing what it writes to stderr.
   *
   * @param args the command line arguments
   * @return the captured stderr and the status code
   */
  private static Run run(String... args) {
    CommandLine cli = IsthmusEntryPoint.createCommandLine();
    StringWriter err = new StringWriter();
    cli.setErr(new PrintWriter(err));
    int statusCode = cli.execute(args);
    cli.getErr().flush();
    return new Run(statusCode, err.toString());
  }

  @Test
  void canProcessQuery() {
    assertEquals(0, run("SELECT 1;").statusCode);
  }

  @Test
  void canProcessQueryWithCreates() {
    assertEquals(0, run("SELECT * FROM foo", "--create", "CREATE TABLE foo(id INT)").statusCode);
  }

  @Test
  void undefinedTableSuggestsCreateOption() {
    Run run = run("SELECT * FROM foo");

    assertEquals(CommandLine.ExitCode.SOFTWARE, run.statusCode);
    run.assertErrContains("Object 'FOO' not found", "-c / --create", "CREATE TABLE FOO");
    run.assertNoStackTrace();
  }

  @Test
  void undefinedColumnIsExplained() {
    Run run = run("SELECT bar FROM foo", "-c", "CREATE TABLE foo(id INT)");

    assertEquals(CommandLine.ExitCode.SOFTWARE, run.statusCode);
    run.assertErrContains("Column 'BAR' not found", "'BAR' is not a column", "--unquotedcasing");
    run.assertNoStackTrace();
  }

  @Test
  void unknownIdentifierInExpressionSuggestsCreateOption() {
    Run run = run("-e", "col + 1");

    assertEquals(CommandLine.ExitCode.SOFTWARE, run.statusCode);
    run.assertErrContains("Unknown identifier 'COL'", "-e / --expression", "-c / --create");
    run.assertNoStackTrace();
  }

  @Test
  void malformedQueryReportsTheParseError() {
    Run run = run("SELECT 1 FROM");

    assertEquals(CommandLine.ExitCode.SOFTWARE, run.statusCode);
    run.assertErrContains("Encountered \"<EOF>\" at line 1, column 13.");
    run.assertNoStackTrace();
  }

  @Test
  void createTableAsQuerySuggestsCreateOption() {
    Run run = run("CREATE TABLE foo(a INT)");

    assertEquals(CommandLine.ExitCode.SOFTWARE, run.statusCode);
    run.assertErrContains("-c / --create takes plain CREATE TABLE statements");
    run.assertNoStackTrace();
  }

  @Test
  void queryPassedToCreateOptionSuggestsQueryArgument() {
    Run run = run("SELECT * FROM foo", "-c", "SELECT 1");

    assertEquals(CommandLine.ExitCode.SOFTWARE, run.statusCode);
    run.assertErrContains(
        "Not a valid CREATE TABLE statement.", "-c / --create takes plain CREATE TABLE statements");
    run.assertNoStackTrace();
  }

  @Test
  void ctasPassedToCreateOptionSuggestsQueryArgument() {
    Run run = run("SELECT * FROM foo", "-c", "CREATE TABLE foo AS SELECT 1");

    assertEquals(CommandLine.ExitCode.SOFTWARE, run.statusCode);
    run.assertErrContains(
        "CTAS not supported.", "-c / --create takes plain CREATE TABLE statements");
    run.assertNoStackTrace();
  }

  @Test
  void missingSqlReportsUsageError() {
    Run run = run("-c", "CREATE TABLE foo(id INT)");

    assertEquals(CommandLine.ExitCode.USAGE, run.statusCode);
    run.assertErrContains("Missing SQL to convert", "Usage: isthmus");
    run.assertNoStackTrace();
  }

  @Test
  void stackTraceOptionKeepsTheStackTrace() {
    Run run = run("SELECT * FROM foo", "--stacktrace");

    assertEquals(CommandLine.ExitCode.SOFTWARE, run.statusCode);
    run.assertErrContains(
        "Object 'FOO' not found", "-c / --create", "org.apache.calcite.runtime", "\tat ");
  }
}
