package io.substrait.isthmus.sql;

import io.substrait.isthmus.ConverterProvider;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Utility class for parsing SQL statements to {@link SqlNode}s using a Substrait flavoured SQL
 * parser. Intended for testing and experimentation.
 */
public class SubstraitSqlStatementParser {

  /**
   * Parse one or more SQL statements to a list of {@link SqlNode}s.
   *
   * @param sqlStatements a string containing one or more SQL statements
   * @return a list of {@link SqlNode}s corresponding to the given statements
   * @throws SqlParseException if there is an error while parsing the SQL statements
   */
  public static List<SqlNode> parseStatements(String sqlStatements) throws SqlParseException {
    return parseStatements(sqlStatements, ConverterProvider.DEFAULT);
  }

  /**
   * Parse one or more SQL statements to a list of {@link SqlNode}s, using the parser settings from
   * the given {@link ConverterProvider}.
   *
   * <p>To use a custom parser configuration, subclass {@link ConverterProvider} and override {@link
   * ConverterProvider#getSqlParserConfig()}.
   *
   * @param sqlStatements a string containing one or more SQL statements
   * @param converterProvider the converter provider whose parser config controls identifier casing
   *     and other parser settings
   * @return a list of {@link SqlNode}s corresponding to the given statements
   * @throws SqlParseException if there is an error while parsing the SQL statements
   */
  public static List<SqlNode> parseStatements(
      String sqlStatements, ConverterProvider converterProvider) throws SqlParseException {
    SqlParser parser = SqlParser.create(sqlStatements, converterProvider.getSqlParserConfig());
    return parser.parseStmtList();
  }
}
