package io.substrait.isthmus.sql;

import java.util.List;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/** Utility class for parsing SQL statements to {@link org.apache.calcite.rel.RelRoot}s */
public class SubstraitStatementParser {

  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          // TODO: switch to Casing.UNCHANGED
          .withUnquotedCasing(Casing.TO_UPPER)
          // use LENIENT conformance to allow for parsing a wide variety of dialects
          .withConformance(SqlConformanceEnum.LENIENT);

  /** Parse one or more statements */
  public static List<SqlNode> parseStatements(String statements) throws SqlParseException {
    SqlParser parser = SqlParser.create(statements, PARSER_CONFIG);
    return parser.parseStmtList();
  }
}
