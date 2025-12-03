package io.substrait.isthmus.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class IsthmusEntryPointTest {

  @Test
  void canProcessQuery() {
    final IsthmusEntryPoint isthmusEntryPoint = new IsthmusEntryPoint();
    final CommandLine cli = new CommandLine(isthmusEntryPoint);
    final int statusCode = cli.execute("SELECT 1;");
    assertEquals(0, statusCode);
  }

  @Test
  void canProcessQueryWithCreates() {
    final IsthmusEntryPoint isthmusEntryPoint = new IsthmusEntryPoint();
    final CommandLine cli = new CommandLine(isthmusEntryPoint);
    final int statusCode = cli.execute("SELECT * FROM foo", "--create", "CREATE TABLE foo(id INT)");
    assertEquals(0, statusCode);
  }
}
