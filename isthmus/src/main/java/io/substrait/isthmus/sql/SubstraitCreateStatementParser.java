package io.substrait.isthmus.sql;

import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.calcite.SubstraitTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;

/** Utility class for parsing CREATE statements into a {@link CalciteCatalogReader} */
public class SubstraitCreateStatementParser {

  private static final RelDataTypeFactory TYPE_FACTORY =
      new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM);

  private static final CalciteConnectionConfig CONNECTION_CONFIG =
      CalciteConnectionConfig.DEFAULT.set(
          CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString());

  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          // To process CREATE statements we must use the SqlDdlParserImpl, as the default
          // parser does not handle them
          .withParserFactory(SqlDdlParserImpl.FACTORY)
          .withUnquotedCasing(Casing.TO_UPPER)
          .withConformance(SqlConformanceEnum.LENIENT);

  private static final CalciteCatalogReader EMPTY_CATALOG =
      new CalciteCatalogReader(
          CalciteSchema.createRootSchema(false),
          Collections.emptyList(),
          TYPE_FACTORY,
          CONNECTION_CONFIG);

  // A validator is needed to convert the types in column declarations to Calcite types
  private static final SqlValidator VALIDATOR =
      new SubstraitSqlValidator(
          // as we are validating CREATE statements, an empty catalog suffices
          EMPTY_CATALOG);

  /**
   * Parses a SQL string containing only CREATE statements into a list of {@link SubstraitTable}s
   *
   * @param createStatements a SQL string containing only CREATE statements
   * @return a list of {@link SubstraitTable}s generated from the CREATE statements
   * @throws SqlParseException
   */
  public static List<SubstraitTable> processCreateStatements(String createStatements)
      throws SqlParseException {
    SqlParser parser = SqlParser.create(createStatements, PARSER_CONFIG);
    List<SubstraitTable> tableList = new ArrayList<>();

    SqlNodeList sqlNode = parser.parseStmtList();
    for (SqlNode parsed : sqlNode) {
      if (!(parsed instanceof SqlCreateTable create)) {
        throw fail("Not a valid CREATE TABLE statement.");
      }

      if (create.name.names.size() > 1) {
        throw fail("Only simple table names are allowed.", create.name.getParserPosition());
      }

      if (create.query != null) {
        throw fail("CTAS not supported.", create.name.getParserPosition());
      }

      List<String> names = new ArrayList<>();
      List<RelDataType> columnTypes = new ArrayList<>();

      for (SqlNode node : create.columnList) {
        if (!(node instanceof SqlColumnDeclaration col)) {
          if (node instanceof SqlKeyConstraint) {
            // key constraints declarations, like primary key declaration, are valid and should not
            // result in parse exceptions. Ignore the constraint declaration.
            continue;
          }

          throw fail("Unexpected column list construction.", node.getParserPosition());
        }

        if (col.name.names.size() != 1) {
          throw fail("Expected simple column names.", col.name.getParserPosition());
        }

        names.add(col.name.names.get(0));
        columnTypes.add(col.dataType.deriveType(VALIDATOR));
      }

      tableList.add(
          new SubstraitTable(
              create.name.names.get(0), TYPE_FACTORY.createStructType(columnTypes, names)));
    }

    return tableList;
  }

  /**
   * Parses one or more SQL strings containing only CREATE statements into a {@link
   * CalciteCatalogReader}
   *
   * @param createStatements a SQL string containing only CREATE statements
   * @return a {@link CalciteCatalogReader} generated from the CREATE statements
   * @throws SqlParseException
   */
  public static CalciteCatalogReader processCreateStatementsToCatalog(String... createStatements)
      throws SqlParseException {
    List<SubstraitTable> tables = new ArrayList<>();
    for (String statement : createStatements) {
      tables.addAll(processCreateStatements(statement));
    }
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    for (SubstraitTable table : tables) {
      rootSchema.add(table.getName(), table);
    }
    List<String> defaultSchema = Collections.emptyList();
    return new CalciteCatalogReader(rootSchema, defaultSchema, TYPE_FACTORY, CONNECTION_CONFIG);
  }

  private static SqlParseException fail(String text, SqlParserPos pos) {
    return new SqlParseException(text, pos, null, null, new RuntimeException("fake lineage"));
  }

  private static SqlParseException fail(String text) {
    return fail(text, SqlParserPos.ZERO);
  }
}
