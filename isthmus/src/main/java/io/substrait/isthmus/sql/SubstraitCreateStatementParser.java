package io.substrait.isthmus.sql;

import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.Utils;
import io.substrait.isthmus.calcite.SubstraitTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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

  /**
   * An empty catalog reader used for validating CREATE statements, configured from {@link
   * ConverterProvider#DEFAULT}.
   */
  public static final CalciteCatalogReader EMPTY_CATALOG =
      createEmptyCatalog(ConverterProvider.DEFAULT);

  /**
   * SQL validator configured for validating CREATE statements against the empty catalog, using
   * {@link ConverterProvider#DEFAULT}.
   */
  public static final SqlValidator VALIDATOR = createValidator(ConverterProvider.DEFAULT);

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
    return processCreateStatements(ConverterProvider.DEFAULT, createStatements);
  }

  /**
   * Parses a SQL string containing only CREATE statements into a list of {@link SubstraitTable}s,
   * using the parser settings from the given {@link ConverterProvider}.
   *
   * <p>This method only supports simple table names without any additional qualifiers. Only used
   * with {@link io.substrait.isthmus.SqlExpressionToSubstrait}.
   *
   * @param converterProvider the converter provider whose parser config controls identifier casing
   *     and other parser settings
   * @param createStatements a SQL string containing only CREATE statements; must not be null
   * @return list of {@link SubstraitTable}s generated from the CREATE statements
   * @throws SqlParseException if parsing fails or statements are invalid
   */
  public static List<SubstraitTable> processCreateStatements(
      @NonNull final ConverterProvider converterProvider, @NonNull final String createStatements)
      throws SqlParseException {
    final List<SubstraitTable> tableList = new ArrayList<>();
    final SqlValidator validator = createValidator(converterProvider);

    final List<SqlNode> sqlNode =
        SubstraitSqlStatementParser.parseStatements(createStatements, converterProvider);
    for (final SqlNode parsed : sqlNode) {
      if (!(parsed instanceof SqlCreateTable)) {
        throw fail("Not a valid CREATE TABLE statement.");
      }

      final SqlCreateTable create = (SqlCreateTable) parsed;

      if (create.name.names.size() > 1) {
        throw fail("Only simple table names are allowed.", create.name.getParserPosition());
      }

      validateCreateTable(create);

      tableList.add(
          createSubstraitTable(
              converterProvider.getTypeFactory(),
              validator,
              create.name.names.get(0),
              create.columnList));
    }

    return tableList;
  }

  /**
   * Parses one or more SQL strings containing only CREATE statements into a {@link
   * CalciteCatalogReader}.
   *
   * <p>This method expects the use of fully qualified table names in the CREATE statements.
   *
   * @param createStatements List of SQL strings containing only CREATE statements, must not be null
   * @return a {@link CalciteCatalogReader} generated from the CREATE statements
   * @throws SqlParseException if there is an error parsing the SQL statements
   */
  public static CalciteCatalogReader processCreateStatementsToCatalog(
      @NonNull final List<String> createStatements) throws SqlParseException {
    return processCreateStatementsToCatalog(createStatements.toArray(new String[0]));
  }

  /**
   * Parses one or more SQL strings containing only CREATE statements into a {@link
   * CalciteCatalogReader}, using the parser settings from the given {@link ConverterProvider}.
   *
   * <p>This method expects the use of fully qualified table names in the CREATE statements.
   *
   * @param converterProvider the converter provider whose parser config controls identifier casing
   *     and other parser settings
   * @param createStatements List of SQL strings containing only CREATE statements, must not be null
   * @return a {@link CalciteCatalogReader} generated from the CREATE statements
   * @throws SqlParseException if there is an error parsing the SQL statements
   */
  public static CalciteCatalogReader processCreateStatementsToCatalog(
      @NonNull final ConverterProvider converterProvider,
      @NonNull final List<String> createStatements)
      throws SqlParseException {
    return processCreateStatementsToCatalog(
        converterProvider, createStatements.toArray(new String[0]));
  }

  /**
   * Parses one or more SQL strings containing only CREATE statements into a {@link
   * CalciteCatalogReader}
   *
   * <p>This method expects the use of fully qualified table names in the CREATE statements.
   *
   * @param createStatements a SQL string containing only CREATE statements, must not be null
   * @return a {@link CalciteCatalogReader} generated from the CREATE statements
   * @throws SqlParseException if parsing fails or statements are invalid
   */
  public static CalciteCatalogReader processCreateStatementsToCatalog(
      @NonNull final String... createStatements) throws SqlParseException {
    return processCreateStatementsToCatalog(ConverterProvider.DEFAULT, createStatements);
  }

  /**
   * Parses one or more SQL strings containing only CREATE statements into a {@link
   * CalciteCatalogReader}, using the parser settings from the given {@link ConverterProvider}.
   *
   * <p>This method expects the use of fully qualified table names in the CREATE statements.
   *
   * @param converterProvider the converter provider whose parser config controls identifier casing
   *     and other parser settings
   * @param createStatements a SQL string containing only CREATE statements, must not be null
   * @return a {@link CalciteCatalogReader} generated from the CREATE statements
   * @throws SqlParseException if parsing fails or statements are invalid
   */
  public static CalciteCatalogReader processCreateStatementsToCatalog(
      @NonNull final ConverterProvider converterProvider, @NonNull final String... createStatements)
      throws SqlParseException {
    final CalciteSchema rootSchema =
        processCreateStatementsToSchema(converterProvider, createStatements);
    final List<String> defaultSchema = Collections.emptyList();
    return new CalciteCatalogReader(
        rootSchema,
        defaultSchema,
        converterProvider.getTypeFactory(),
        converterProvider.getCalciteConnectionConfig());
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
   * Rejects the CREATE TABLE statements that carry no table schema to build a {@link
   * SubstraitTable} from. Calcite's DDL grammar makes both the column list and the {@code AS query}
   * optional and independent of each other, so a statement can arrive with either part missing.
   *
   * @param create the parsed CREATE TABLE statement; must not be null
   * @throws SqlParseException if the statement defines its columns by a query, or does not define
   *     them at all
   */
  private static void validateCreateTable(@NonNull final SqlCreateTable create)
      throws SqlParseException {
    if (create.query != null) {
      throw fail("CTAS not supported.", create.name.getParserPosition());
    }

    if (create.columnList == null) {
      throw fail("Column definitions are required.", create.name.getParserPosition());
    }
  }

  /**
   * Parses one or more SQL strings containing only CREATE statements into a {@link CalciteSchema}
   * using the given provider's parser config.
   */
  private static CalciteSchema processCreateStatementsToSchema(
      @NonNull final ConverterProvider converterProvider, @NonNull final String... createStatements)
      throws SqlParseException {
    final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    final SqlValidator validator = createValidator(converterProvider);

    for (final String statement : createStatements) {
      final List<SqlNode> sqlNode =
          SubstraitSqlStatementParser.parseStatements(statement, converterProvider);
      for (final SqlNode parsed : sqlNode) {
        if (!(parsed instanceof SqlCreateTable)) {
          throw fail("Not a valid CREATE TABLE statement.");
        }

        final SqlCreateTable create = (SqlCreateTable) parsed;

        validateCreateTable(create);

        final List<String> names = create.name.names;

        final CalciteSchema schema =
            Utils.createCalciteSchemaFromNames(rootSchema, names.subList(0, names.size() - 1));

        // Create the table if it is not present
        final String tableName = names.get(names.size() - 1);
        final CalciteSchema.TableEntry table = schema.getTable(tableName, false);
        if (table == null) {
          schema.add(
              tableName,
              createSubstraitTable(
                  converterProvider.getTypeFactory(), validator, tableName, create.columnList));
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
   * @param typeFactory the type factory used to build the table's row type; must not be null
   * @param validator the validator used to derive the column types; must not be null
   * @param tableName the table name to use; must not be null
   * @param columnList the {@link SqlNodeList} containing {@link SqlColumnDeclaration}s to build the
   *     table schema from; must not be null
   * @return the constructed {@link SubstraitTable}
   * @throws SqlParseException if the column list contains unexpected nodes or invalid names
   */
  private static SubstraitTable createSubstraitTable(
      @NonNull final RelDataTypeFactory typeFactory,
      @NonNull final SqlValidator validator,
      @NonNull final String tableName,
      @NonNull final SqlNodeList columnList)
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
      columnTypes.add(col.dataType.deriveType(validator));
    }

    return new SubstraitTable(tableName, typeFactory.createStructType(columnTypes, names));
  }

  /**
   * Creates an empty catalog reader for validating CREATE statements, using the type factory and
   * connection configuration from the given {@link ConverterProvider}.
   *
   * @param converterProvider the converter provider supplying the type factory and connection
   *     configuration; must not be null
   * @return an empty {@link CalciteCatalogReader}
   */
  private static CalciteCatalogReader createEmptyCatalog(
      @NonNull final ConverterProvider converterProvider) {
    return new CalciteCatalogReader(
        CalciteSchema.createRootSchema(false),
        List.of(),
        converterProvider.getTypeFactory(),
        converterProvider.getCalciteConnectionConfig());
  }

  /**
   * Creates a SQL validator for deriving the column types of CREATE statements, configured from the
   * given {@link ConverterProvider}. As only CREATE statements are validated, an empty catalog
   * suffices.
   *
   * @param converterProvider the converter provider supplying the validator configuration; must not
   *     be null
   * @return a {@link SqlValidator} for validating CREATE statements
   */
  private static SqlValidator createValidator(@NonNull final ConverterProvider converterProvider) {
    return new SubstraitSqlValidator(
        createEmptyCatalog(converterProvider), converterProvider.getSqlOperatorTable());
  }
}
