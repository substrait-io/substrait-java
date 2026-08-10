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

  /**
   * Matches Calcite's complaint about a table (or other object) that the catalog does not hold. The
   * second group holds the schema it looked in, for the qualified form of the message.
   */
  private static final Pattern OBJECT_NOT_FOUND =
      Pattern.compile("(?:Object|Table) '([^']+)' not found(?: within '([^']+)')?");

  /** Matches Calcite's complaint about a column that none of the known tables holds. */
  private static final Pattern COLUMN_NOT_FOUND = Pattern.compile("Column '([^']+)' not found");

  /** Matches Calcite's complaint about an identifier that an expression cannot be resolved to. */
  private static final Pattern UNKNOWN_IDENTIFIER = Pattern.compile("Unknown identifier '([^']+)'");

  /** Matches Calcite's complaint about a query given where an expression was expected. */
  private static final Pattern STATEMENT_AS_EXPRESSION =
      Pattern.compile("Incorrect syntax near the keyword '(?:SELECT|WITH|VALUES|TABLE)'");

  /** The message the CREATE statement parser reports for anything that is not a CREATE TABLE. */
  private static final String NOT_A_CREATE_TABLE = "Not a valid CREATE TABLE statement.";

  /** The message the CREATE statement parser reports for a CREATE TABLE AS SELECT. */
  private static final String CTAS_NOT_SUPPORTED = "CTAS not supported.";

  /** The message the CREATE statement parser reports for a CREATE TABLE without a column list. */
  private static final String COLUMNS_REQUIRED = "Column definitions are required.";

  /** The message the DDL converter reports for a CREATE TABLE without a query. */
  private static final String CTAS_ONLY = "Only create table as select statements are supported";

  // The hints are hard-wrapped for a terminal rather than joined into single long lines.

  private static final String CREATE_HINT =
      """
      Hint: table definitions are not part of the query. Pass a CREATE TABLE
      statement for each table it references using -c / --create:

        isthmus -c "CREATE TABLE %1$s (col1 INT, col2 VARCHAR)" "SELECT * FROM %1$s\"""";

  private static final String COLUMN_HINT =
      """
      Hint: '%1$s' is not a column of any table defined with -c / --create. Check
      the column names in the CREATE TABLE statement.""";

  private static final String COLUMN_WITHOUT_CREATE_HINT =
      """
      Hint: '%1$s' is not a column of any table, and no table was defined. Pass a
      CREATE TABLE statement for each table the query reads using -c / --create:

        isthmus -c "CREATE TABLE T (%1$s INT)" "SELECT %1$s FROM T\"""";

  private static final String EXPRESSION_HINT =
      """
      Hint: identifiers in a -e / --expression must be columns of a table defined
      with -c / --create:

        isthmus -c "CREATE TABLE T (%1$s INT)" -e "%1$s + 1\"""";

  private static final String EXPRESSION_ARGUMENT_HINT =
      """
      Hint: -e / --expression consumes every following argument, so a query
      written after it is parsed as an expression. Pass the query on its own:

        isthmus "SELECT * FROM FOO\"""";

  private static final String QUERY_ARGUMENT_HINT =
      """
      Hint: -c / --create takes plain CREATE TABLE statements; the query itself is
      the first argument:

        isthmus -c "CREATE TABLE FOO (col1 INT)" "SELECT * FROM FOO\"""";

  private static final String COLUMN_LIST_HINT =
      """
      Hint: -c / --create needs the columns of the table, with their types:

        isthmus -c "CREATE TABLE FOO (col1 INT, col2 VARCHAR)" "SELECT * FROM FOO\"""";

  /** Appended to the hints whose advice depends on how unquoted identifiers are cased. */
  private static final String CASING_NOTE =
      "\n\nUnquoted identifiers are upper-cased unless --unquotedcasing says otherwise.";

  @Override
  public int handleExecutionException(
      Exception ex, CommandLine cmd, CommandLine.ParseResult parseResult) throws Exception {
    if (!isInputError(ex)) {
      // Not something the user can act on: keep the stack trace, it belongs in a bug report.
      throw ex;
    }

    PrintWriter err = cmd.getErr();
    err.println(cmd.getColorScheme().errorText("Error: " + ex.getMessage()));
    Optional<String> hint = hint(ex, parseResult);
    if (hint.isPresent()) {
      err.println();
      err.println(hint.get());
    }
    err.flush();

    if (parseResult.hasMatchedOption("--stacktrace")) {
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
    if (ex instanceof SqlParseException) {
      // Calcite funnels whatever the parser threw into a SqlParseException, leaving the position
      // null unless the cause was a grammar or lexer error
      // (SqlAbstractParserImpl.convertException). A missing position therefore means a defect
      // rather than a problem with the SQL.
      return ((SqlParseException) ex).getPos() != null;
    }
    // CalciteContextException is, by construction, a complaint about the SQL at a line and column.
    return ex instanceof CalciteContextException || isPlainCreateTableQuery(ex);
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
   * Returns the hint to print for the given exception, if the message and the options it was given
   * identify a mistake we can advise on.
   *
   * @param ex the exception thrown while converting
   * @param parseResult the parsed command line the failure came from
   * @return the hint to print below the error message, or empty if the mistake is not recognized
   */
  private static Optional<String> hint(
      final Exception ex, final CommandLine.ParseResult parseResult) {
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
    if (message.contains(COLUMNS_REQUIRED)) {
      return Optional.of(COLUMN_LIST_HINT);
    }

    boolean expressions = parseResult.hasMatchedOption("-e");
    if (expressions && STATEMENT_AS_EXPRESSION.matcher(message).find()) {
      return Optional.of(EXPRESSION_ARGUMENT_HINT);
    }

    Matcher objectNotFound = OBJECT_NOT_FOUND.matcher(message);
    if (objectNotFound.find()) {
      return Optional.of(
          withCasingNote(CREATE_HINT.formatted(objectName(objectNotFound)), parseResult));
    }
    Matcher columnNotFound = COLUMN_NOT_FOUND.matcher(message);
    if (columnNotFound.find()) {
      String hint = parseResult.hasMatchedOption("-c") ? COLUMN_HINT : COLUMN_WITHOUT_CREATE_HINT;
      return Optional.of(withCasingNote(hint.formatted(columnNotFound.group(1)), parseResult));
    }
    Matcher unknownIdentifier = UNKNOWN_IDENTIFIER.matcher(message);
    if (expressions && unknownIdentifier.find()) {
      // Without -e this is a complaint about a -c statement, where naming the identifier as a
      // column of a new table would be the wrong advice.
      return Optional.of(EXPRESSION_HINT.formatted(unknownIdentifier.group(1)));
    }
    return Optional.empty();
  }

  /**
   * Returns the name to suggest defining, qualified with the schema Calcite looked in when the
   * message names one.
   *
   * @param objectNotFound the match of {@link #OBJECT_NOT_FOUND} against the error message
   * @return the object name to name in the hint
   */
  private static String objectName(final Matcher objectNotFound) {
    String schema = objectNotFound.group(2);
    return schema == null ? objectNotFound.group(1) : schema + "." + objectNotFound.group(1);
  }

  /**
   * Appends the note about identifier casing to the given hint, unless the casing was chosen on the
   * command line and the note would be both wrong and beside the point.
   *
   * @param hint the hint to print below the error message
   * @param parseResult the parsed command line the failure came from
   * @return the hint, with the casing note where it applies
   */
  private static String withCasingNote(
      final String hint, final CommandLine.ParseResult parseResult) {
    return parseResult.hasMatchedOption("--unquotedcasing") ? hint : hint + CASING_NOTE;
  }
}
