package io.substrait.examples;

import io.substrait.isthmus.calcite.SubstraitTable;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.parser.SqlParseException;

/** Helper functions for schemas. */
public final class SchemaHelper {

  private SchemaHelper() {}

  /**
   * Parses one or more SQL strings containing only CREATE statements into a {@link
   * CalciteCatalogReader}
   *
   * @param createStatements a SQL string containing only CREATE statements
   * @return a {@link CalciteCatalogReader} generated from the CREATE statements
   * @throws SqlParseException if the sql can not be parsed
   */
  public static CalciteSchema processCreateStatementsToSchema(final List<String> createStatements)
      throws SqlParseException {

    final List<SubstraitTable> tables = new ArrayList<>();
    for (final String statement : createStatements) {
      tables.addAll(SubstraitCreateStatementParser.processCreateStatements(statement));
    }

    final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    for (final SubstraitTable table : tables) {
      rootSchema.add(table.getName(), table);
    }

    return rootSchema;
  }
}
