package io.substrait.isthmus.sql;

import io.substrait.isthmus.SqlConverterBase;
import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.Utils;
import io.substrait.isthmus.calcite.SubstraitTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Utility class for parsing CREATE statements into a {@link CalciteCatalogReader} */
public class SubstraitCreateStatementParser {

  /** An empty catalog reader used for validating CREATE statements. */
  public static final CalciteCatalogReader EMPTY_CATALOG =
      new CalciteCatalogReader(
          CalciteSchema.createRootSchema(false),
          List.of(),
          SubstraitTypeSystem.TYPE_FACTORY,
          SqlConverterBase.CONNECTION_CONFIG);

  /** SQL validator configured for validating CREATE statements against the empty catalog. */
  public static final SqlValidator VALIDATOR =
      new SubstraitSqlValidator(
          // as we are validating CREATE statements, an empty catalog suffices
          EMPTY_CATALOG);

  /**
   * Parses a SQL string containing only CREATE statements into a list of {@link SubstraitTable}s.
   *
   * <p>This method only supports simple table names without any additional qualifiers. Only used
   * with {@link io.substrait.isthmus.SqlExpressionToSubstrait}.
   *
   * @param createStatements a SQL string containing only CREATE statements; must not be null
   * @return list of {@link SubstraitTable}s generated from the CREATE statements
   * @throws SqlParseException if parsing fails or statements are invalid
   */
  public static List<SubstraitTable> processCreateStatements(@NonNull final String createStatements)
      throws SqlParseException {
    final List<SubstraitTable> tableList = new ArrayList<>();

    final List<SqlNode> sqlNode = SubstraitSqlStatementParser.parseStatements(createStatements);
    for (final SqlNode parsed : sqlNode) {
      if (!(parsed instanceof SqlCreateTable)) {
        throw fail("Not a valid CREATE TABLE statement.");
      }

      final SqlCreateTable create = (SqlCreateTable) parsed;

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
   * CalciteCatalogReader}.
   *
   * <p>This method expects the use of fully qualified table names in the CREATE statements.
   *
   * @param createStatements one or more SQL strings containing only CREATE statements; must not be
   *     null
   * @return a {@link CalciteCatalogReader} generated from the CREATE statements
   * @throws SqlParseException if parsing fails or statements are invalid
   */
  public static CalciteCatalogReader processCreateStatementsToCatalog(
      @NonNull final String... createStatements) throws SqlParseException {
    final CalciteSchema rootSchema = processCreateStatementsToSchema(createStatements);
    final List<String> defaultSchema = Collections.emptyList();
    return new CalciteCatalogReader(
        rootSchema,
        defaultSchema,
        SubstraitTypeSystem.TYPE_FACTORY,
        SqlConverterBase.CONNECTION_CONFIG);
  }

  /**
   * Creates a new {@link SqlParseException} with the given message and {@link SqlParserPos}.
   *
   * @param message the exception message; may be null
   * @param pos the position where this error occurred; may be null
   * @return a {@link SqlParseException} with the given message and position
   */
  private static SqlParseException fail(
      @Nullable final String message, @Nullable final SqlParserPos pos) {
    return new SqlParseException(message, pos, null, null, new RuntimeException("fake lineage"));
  }

  /**
   * Creates a new {@link SqlParseException} with the given message.
   *
   * @param message the exception message; may be null
   * @return a {@link SqlParseException} with the given message
   */
  private static SqlParseException fail(@Nullable final String message) {
    return fail(message, SqlParserPos.ZERO);
  }

  /**
   * Parses one or more SQL strings containing only CREATE statements into a {@link CalciteSchema}.
   *
   * @param createStatements one or more SQL strings containing only CREATE statements; must not be
   *     null
   * @return a {@link CalciteSchema} generated from the CREATE statements
   * @throws SqlParseException if parsing fails or statements are invalid
   */
  private static CalciteSchema processCreateStatementsToSchema(
      @NonNull final String... createStatements) throws SqlParseException {
    final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

    for (final String statement : createStatements) {
      final List<SqlNode> sqlNode = SubstraitSqlStatementParser.parseStatements(statement);
      for (final SqlNode parsed : sqlNode) {
        if (!(parsed instanceof SqlCreateTable)) {
          throw fail("Not a valid CREATE TABLE statement.");
        }

        final SqlCreateTable create = (SqlCreateTable) parsed;
        final List<String> names = create.name.names;

        final CalciteSchema schema =
            Utils.createCalciteSchemaFromNames(rootSchema, names.subList(0, names.size() - 1));

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
   * @param tableName the table name to use; must not be null
   * @param columnList the {@link SqlNodeList} containing {@link SqlColumnDeclaration}s to build the
   *     table schema from; must not be null
   * @return the constructed {@link SubstraitTable}
   * @throws SqlParseException if the column list contains unexpected nodes or invalid names
   */
  private static SubstraitTable createSubstraitTable(
      @NonNull final String tableName, @NonNull final SqlNodeList columnList)
      throws SqlParseException {
    final List<String> names = new ArrayList<>();
    final List<RelDataType> columnTypes = new ArrayList<>();

    for (final SqlNode node : columnList) {
      if (!(node instanceof SqlColumnDeclaration)) {
        if (node instanceof SqlKeyConstraint) {
          // key constraints declarations, like primary key declaration, are valid and should not
          // result in parse exceptions. Ignore the constraint declaration.
          continue;
        }

        throw fail("Unexpected column list construction.", node.getParserPosition());
      }

      final SqlColumnDeclaration col = (SqlColumnDeclaration) node;

      if (col.name.names.size() != 1) {
        throw fail("Expected simple column names.", col.name.getParserPosition());
      }

      names.add(col.name.names.get(0));
      columnTypes.add(col.dataType.deriveType(VALIDATOR));
    }

    return new SubstraitTable(
        tableName, SubstraitTypeSystem.TYPE_FACTORY.createStructType(columnTypes, names));
  }
}
