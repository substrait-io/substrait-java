package io.substrait.isthmus.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class IsthmusEntryPointTest {

  @Test
  void canProcessQuery() {
    IsthmusEntryPoint isthmusEntryPoint = new IsthmusEntryPoint();
    CommandLine cli = new CommandLine(isthmusEntryPoint);
    int statusCode = cli.execute("SELECT 1;");
    assertEquals(0, statusCode);
  }

  @Test
  void canProcessQueryWithCreates() {
    IsthmusEntryPoint isthmusEntryPoint = new IsthmusEntryPoint();
    CommandLine cli = new CommandLine(isthmusEntryPoint);
    int statusCode = cli.execute("SELECT * FROM foo", "--create", "CREATE TABLE foo(id INT)");
    assertEquals(0, statusCode);
  }
}
