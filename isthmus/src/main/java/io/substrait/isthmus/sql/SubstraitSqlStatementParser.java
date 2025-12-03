package io.substrait.isthmus.sql;

import java.util.List;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * Utility class for parsing SQL statements to {@link SqlNode}s using a Substrait flavoured SQL
 * parser. Intended for testing and experimentation.
 */
public class SubstraitSqlStatementParser {

  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          // TODO: switch to Casing.UNCHANGED
          .withUnquotedCasing(Casing.TO_UPPER)
          // use LENIENT conformance to allow for parsing a wide variety of dialects
          .withConformance(SqlConformanceEnum.LENIENT)
          .withParserFactory(SqlDdlParserImpl.FACTORY);

  /**
   * Parse one or more SQL statements to a list of {@link SqlNode}s.
   *
   * @param sqlStatements a string containing one or more SQL statements
   * @return a list of {@link SqlNode}s corresponding to the given statements
   * @throws SqlParseException if there is an error while parsing the SQL statements
   */
  public static List<SqlNode> parseStatements(final String sqlStatements) throws SqlParseException {
    final SqlParser parser = SqlParser.create(sqlStatements, PARSER_CONFIG);
    return parser.parseStmtList();
  }
}
