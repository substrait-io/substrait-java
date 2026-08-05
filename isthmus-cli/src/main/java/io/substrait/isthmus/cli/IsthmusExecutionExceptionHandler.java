package io.substrait.isthmus.cli;

import java.io.PrintWriter;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import picocli.CommandLine;

/**
 * Reports mistakes in the SQL handed to the CLI as a short message, with a hint about the option to
 * reach for whenever the mistake can be identified.
 *
 * <p>Only failures that are recognizably caused by the input are reported this way. Anything else
 * is rethrown so that its stack trace is still printed, as is the full stack trace of a recognized
 * failure when {@code --stacktrace} is given.
 */
class IsthmusExecutionExceptionHandler implements CommandLine.IExecutionExceptionHandler {

  /** Matches Calcite's complaint about a table (or other object) that the catalog does not hold. */
  private static final Pattern OBJECT_NOT_FOUND =
      Pattern.compile("(?:Object|Table) '([^']+)' not found");

  /** Matches Calcite's complaint about a column that none of the known tables holds. */
  private static final Pattern COLUMN_NOT_FOUND = Pattern.compile("Column '([^']+)' not found");

  /** Matches Calcite's complaint about an identifier that an expression cannot be resolved to. */
  private static final Pattern UNKNOWN_IDENTIFIER = Pattern.compile("Unknown identifier '([^']+)'");

  /** The message the CREATE statement parser reports for anything that is not a CREATE TABLE. */
  private static final String NOT_A_CREATE_TABLE = "Not a valid CREATE TABLE statement.";

  /** The message the CREATE statement parser reports for a CREATE TABLE AS SELECT. */
  private static final String CTAS_NOT_SUPPORTED = "CTAS not supported.";

  /** The message the DDL converter reports for a CREATE TABLE without a query. */
  private static final String CTAS_ONLY = "Only create table as select statements are supported";

  // The hints are hard-wrapped for a terminal rather than joined into single long lines.

  private static final String CREATE_HINT =
      """
      Hint: table definitions are not part of the query. Pass a CREATE TABLE
      statement for each table it references using -c / --create:

        isthmus -c "CREATE TABLE %1$s (col1 INT, col2 VARCHAR)" "SELECT * FROM %1$s"

      Unquoted identifiers are upper-cased unless --unquotedcasing says otherwise.""";

  private static final String COLUMN_HINT =
      """
      Hint: '%s' is not a column of any table defined with -c / --create. Check
      the column names in the CREATE TABLE statement; unquoted identifiers are
      upper-cased unless --unquotedcasing says otherwise.""";

  private static final String EXPRESSION_HINT =
      """
      Hint: identifiers in a -e / --expression must be columns of a table defined
      with -c / --create:

        isthmus -c "CREATE TABLE T (%1$s INT)" -e "%1$s + 1\"""";

  private static final String QUERY_ARGUMENT_HINT =
      """
      Hint: -c / --create takes plain CREATE TABLE statements; the query itself is
      the first argument:

        isthmus -c "CREATE TABLE FOO (col1 INT)" "SELECT * FROM FOO\"""";

  @Override
  public int handleExecutionException(
      Exception ex, CommandLine cmd, CommandLine.ParseResult parseResult) throws Exception {
    if (!isInputError(ex)) {
      // Not something the user can act on: keep the stack trace, it belongs in a bug report.
      throw ex;
    }

    PrintWriter err = cmd.getErr();
    err.println(cmd.getColorScheme().errorText("Error: " + ex.getMessage()));
    Optional<String> hint = hint(ex);
    if (hint.isPresent()) {
      err.println();
      err.println(hint.get());
    }
    err.flush();

    if (stackTraceRequested(cmd)) {
      throw ex;
    }
    return cmd.getCommandSpec().exitCodeOnExecutionException();
  }

  /**
   * Reports whether the given exception was caused by the SQL given to the CLI rather than by a
   * defect.
   *
   * @param ex the exception thrown while converting
   * @return true if the exception describes a problem with the input
   */
  private static boolean isInputError(final Exception ex) {
    // CalciteContextException is, by construction, a complaint about the SQL at a line and column.
    return ex instanceof SqlParseException
        || ex instanceof CalciteContextException
        || isPlainCreateTableQuery(ex);
  }

  /**
   * Reports whether the given exception is the DDL converter's rejection of a CREATE TABLE
   * statement passed as the query.
   *
   * @param ex the exception thrown while converting
   * @return true if a plain CREATE TABLE statement was given as the query
   */
  private static boolean isPlainCreateTableQuery(final Exception ex) {
    return ex instanceof IllegalArgumentException && CTAS_ONLY.equals(ex.getMessage());
  }

  /**
   * Returns the hint to print for the given exception, if its message identifies a mistake we can
   * advise on.
   *
   * @param ex the exception thrown while converting
   * @return the hint to print below the error message, or empty if the mistake is not recognized
   */
  private static Optional<String> hint(final Exception ex) {
    if (isPlainCreateTableQuery(ex)) {
      return Optional.of(QUERY_ARGUMENT_HINT);
    }

    String message = ex.getMessage();
    if (message == null) {
      return Optional.empty();
    }
    if (message.contains(NOT_A_CREATE_TABLE) || message.contains(CTAS_NOT_SUPPORTED)) {
      return Optional.of(QUERY_ARGUMENT_HINT);
    }

    Matcher objectNotFound = OBJECT_NOT_FOUND.matcher(message);
    if (objectNotFound.find()) {
      return Optional.of(CREATE_HINT.formatted(objectNotFound.group(1)));
    }
    Matcher columnNotFound = COLUMN_NOT_FOUND.matcher(message);
    if (columnNotFound.find()) {
      return Optional.of(COLUMN_HINT.formatted(columnNotFound.group(1)));
    }
    Matcher unknownIdentifier = UNKNOWN_IDENTIFIER.matcher(message);
    if (unknownIdentifier.find()) {
      return Optional.of(EXPRESSION_HINT.formatted(unknownIdentifier.group(1)));
    }
    return Optional.empty();
  }

  /**
   * Reports whether the full stack trace was asked for on the command line.
   *
   * @param cmd the command line that failed
   * @return true if {@code --stacktrace} was given
   */
  private static boolean stackTraceRequested(final CommandLine cmd) {
    Object command = cmd.getCommand();
    return command instanceof IsthmusEntryPoint
        && ((IsthmusEntryPoint) command).isStackTraceRequested();
  }
}
