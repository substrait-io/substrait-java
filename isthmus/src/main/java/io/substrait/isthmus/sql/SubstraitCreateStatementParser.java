package io.substrait.isthmus.sql;

import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.Utils;
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
import org.jspecify.annotations.Nullable;

/** Utility class for parsing CREATE statements into a {@link CalciteCatalogReader} */
public class SubstraitCreateStatementParser {

  protected static final RelDataTypeFactory TYPE_FACTORY =
      new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM);

  protected static final CalciteConnectionConfig CONNECTION_CONFIG =
      CalciteConnectionConfig.DEFAULT.set(
          CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString());

  protected static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          // To process CREATE statements we must use the SqlDdlParserImpl, as the default
          // parser does not handle them
          .withParserFactory(SqlDdlParserImpl.FACTORY)
          .withUnquotedCasing(Casing.TO_UPPER)
          .withConformance(SqlConformanceEnum.LENIENT);

  protected static final CalciteCatalogReader EMPTY_CATALOG =
      new CalciteCatalogReader(
          CalciteSchema.createRootSchema(false), List.of(), TYPE_FACTORY, CONNECTION_CONFIG);

  // A validator is needed to convert the types in column declarations to Calcite types
  protected static final SqlValidator VALIDATOR =
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
      if (!(parsed instanceof SqlCreateTable)) {
        throw fail("Not a valid CREATE TABLE statement.");
      }

      SqlCreateTable create = (SqlCreateTable) parsed;

      if (create.name.names.size() > 1) {
        throw fail("Only simple table names are allowed.", create.name.getParserPosition());
      }

      if (create.query != null) {
        throw fail("CTAS not supported.", create.name.getParserPosition());
      }

      tableList.add(createSubstraitTable(create.name.names.get(0), create.columnList));
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
    CalciteSchema rootSchema = processCreateStatementsToSchema(createStatements);
    List<String> defaultSchema = Collections.emptyList();
    return new CalciteCatalogReader(rootSchema, defaultSchema, TYPE_FACTORY, CONNECTION_CONFIG);
  }

  /**
   * Creates a new {@link SqlParseException} with the given message and {@link SqlParserPos}.
   *
   * @param message the exception message, may be null
   * @param pos the position where this error occured, may be null
   * @return the {@link SqlParseException} with the given message and {@link SqlParserPos}
   */
  protected static SqlParseException fail(@Nullable String message, @Nullable SqlParserPos pos) {
    return new SqlParseException(message, pos, null, null, new RuntimeException("fake lineage"));
  }

  /**
   * Creates a new {@link SqlParseException} with the given message.
   *
   * @param message the exception message, may be null
   * @return the {@link SqlParseException} with the given message
   */
  protected static SqlParseException fail(@Nullable String message) {
    return fail(message, SqlParserPos.ZERO);
  }

  /**
   * Parses one or more SQL strings containing only CREATE statements into a {@link CalciteSchema}.
   *
   * @param createStatements a SQL string containing only CREATE statements
   * @return a {@link CalciteSchema} generated from the CREATE statements
   * @throws SqlParseException
   */
  protected static CalciteSchema processCreateStatementsToSchema(final String... createStatements)
      throws SqlParseException {
    final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

    for (final String statement : createStatements) {
      final SqlParser parser = SqlParser.create(statement, PARSER_CONFIG);

      final SqlNodeList sqlNode = parser.parseStmtList();
      for (final SqlNode parsed : sqlNode) {
        if (!(parsed instanceof SqlCreateTable)) {
          throw fail("Not a valid CREATE TABLE statement.");
        }

        final SqlCreateTable create = (SqlCreateTable) parsed;
        final List<String> names = create.name.names;

        CalciteSchema schema = Utils.createCalciteSchemaFromNames(rootSchema, names);

        // Create the table if it is not present
        final String tableName = names.get(names.size() - 1);
        final CalciteSchema.TableEntry table = schema.getTable(tableName, false);
        if (table == null) {
          schema.add(tableName, createSubstraitTable(tableName, create.columnList));
        } else {
          throw fail("Table must not be defined more than once", parsed.getParserPosition());
        }
      }
    }

    return rootSchema;
  }

  /**
   * Creates a new {@link SubstraitTable} with the given table name and the table schema from the
   * given {@link SqlNodeList} containing {@link SqlColumnDeclaration}s.
   *
   * @param tableName the table name to use
   * @param columnList the {@link SqlNodeList} containing {@link SqlColumnDeclaration}s to create
   *     the table schema from
   * @return the {@link SubstraitTable}
   * @throws SqlParseException
   */
  protected static SubstraitTable createSubstraitTable(String tableName, SqlNodeList columnList)
      throws SqlParseException {
    List<String> names = new ArrayList<>();
    List<RelDataType> columnTypes = new ArrayList<>();

    for (SqlNode node : columnList) {
      if (!(node instanceof SqlColumnDeclaration)) {
        if (node instanceof SqlKeyConstraint) {
          // key constraints declarations, like primary key declaration, are valid and should not
          // result in parse exceptions. Ignore the constraint declaration.
          continue;
        }

        throw fail("Unexpected column list construction.", node.getParserPosition());
      }

      SqlColumnDeclaration col = (SqlColumnDeclaration) node;

      if (col.name.names.size() != 1) {
        throw fail("Expected simple column names.", col.name.getParserPosition());
      }

      names.add(col.name.names.get(0));
      columnTypes.add(col.dataType.deriveType(VALIDATOR));
    }

    return new SubstraitTable(tableName, TYPE_FACTORY.createStructType(columnTypes, names));
  }
}
