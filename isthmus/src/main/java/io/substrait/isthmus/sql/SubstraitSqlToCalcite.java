package io.substrait.isthmus.sql;

import io.substrait.isthmus.FeatureBoard;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.operation.CalciteOperation;
import io.substrait.isthmus.operation.CalciteOperationBuilder;
import io.substrait.isthmus.operation.RelationalOperation;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/**
 * Substrait flavoured SQL processor provided as a utility for testing and experimentation,
 * utilizing {@link SubstraitSqlStatementParser} and {@link SubstraitSqlValidator}
 */
public class SubstraitSqlToCalcite {

  protected static SqlParser.Config createDefaultParserConfig() {
    return SqlParser.Config.DEFAULT
        .withUnquotedCasing(createDefaultFeatureBoard().unquotedCasing())
        .withParserFactory(SqlDdlParserImpl.FACTORY)
        .withConformance(SqlConformanceEnum.LENIENT);
  }

  protected static FeatureBoard createDefaultFeatureBoard() {
    return ImmutableFeatureBoard.builder().build();
  }

  protected static SqlToRelConverter createSqlToRelConverter(
      SqlValidator validator, Prepare.CatalogReader catalogReader, RelOptCluster relOptCluster) {
    return new SqlToRelConverter(
        null,
        validator,
        catalogReader,
        relOptCluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.CONFIG);
  }

  protected static RelRoot parseRelationalExpression(SqlToRelConverter converter, SqlNode sqlNode) {
    RelRoot converted = converter.convertQuery(sqlNode, true, true);
    return converted.withRel(removeRedundantProjects(converted.rel));
  }

  protected static RelOptCluster createDefaultRelOptCluster() {
    RexBuilder rexBuilder =
        new RexBuilder(new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM));
    HepProgram program = HepProgram.builder().build();
    RelOptPlanner emptyPlanner = new HepPlanner(program);
    return RelOptCluster.create(emptyPlanner, rexBuilder);
  }

  protected static RelNode removeRedundantProjects(RelNode root) {
    // The Calcite RelBuilder, when constructing Project that does not modify its inputs in any way,
    // simply elides it. The PROJECT_REMOVE rule can be used to remove such projects from Rel trees.
    // This facilitates roundtrip testing.
    HepProgram program = HepProgram.builder().addRuleInstance(CoreRules.PROJECT_REMOVE).build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(root);
    return planner.findBestExp();
  }

  protected static CalciteOperation convert(
      SqlNode sqlNode,
      Prepare.CatalogReader catalogReader,
      SqlValidator validator,
      RelOptCluster cluster) {
    SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader, cluster);
    CalciteOperationBuilder operationBuilder =
        new CalciteOperationBuilder(converter, SubstraitSqlToCalcite::parseRelationalExpression);
    return sqlNode.accept(operationBuilder);
  }

  /**
   * Converts a SQL statement to a Calcite {@link RelRoot}.
   *
   * @param sqlStatement a SQL statement string
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statement
   * @return a {@link RelRoot} corresponding to the given SQL statement
   * @throws SqlParseException if there is an error while parsing the SQL statement
   */
  public static RelRoot convertRelationalQuery(
      String sqlStatement, Prepare.CatalogReader catalogReader) throws SqlParseException {
    return convertRelationalQuery(
        sqlStatement,
        catalogReader,
        new SubstraitSqlValidator(catalogReader),
        createDefaultParserConfig(),
        createDefaultRelOptCluster());
  }

  /**
   * Converts a SQL statement to a Calcite {@link RelRoot}.
   *
   * @param sqlStatement a SQL statement string
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statement
   * @param validator the {@link SqlValidator} used to validate the SQL statement. Allows for
   *     additional control of SQL functions and operators via {@link
   *     SqlValidator#getOperatorTable()}
   * @param parserConfig the {@link SqlParser.Config} used for parsing the Sql statement
   * @param cluster the {@link RelOptCluster} used when creating {@link RelNode}s during statement
   *     processing. Calcite expects that the {@link RelOptCluster} used during statement processing
   *     is the same as that used during query optimization.
   * @return a {@link RelRoot} corresponding to the given SQL statement
   * @throws SqlParseException if there is an error while parsing the SQL statement
   */
  public static RelRoot convertRelationalQuery(
      String sqlStatement,
      Prepare.CatalogReader catalogReader,
      SqlValidator validator,
      SqlParser.Config parserConfig,
      RelOptCluster cluster)
      throws SqlParseException {
    CalciteOperation calciteOperation =
        convertQuery(sqlStatement, catalogReader, validator, parserConfig, cluster);
    if (calciteOperation instanceof RelationalOperation) {
      return ((RelationalOperation) calciteOperation).getRelRoot();
    }
    throw new IllegalArgumentException(
        String.format("Non-relational algebra statement: %s", sqlStatement));
  }

  /**
   * Converts a SQL statement to a CalciteOperation {@link CalciteOperation}.
   *
   * @param sqlStatement a SQL statement
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statement
   * @return {@link CalciteOperation} corresponding to the given SQL statement
   * @throws SqlParseException if there is an error while parsing the SQL statement string
   */
  public static CalciteOperation convertQuery(
      String sqlStatement, Prepare.CatalogReader catalogReader) throws SqlParseException {
    return convertQuery(
        sqlStatement,
        catalogReader,
        new SubstraitSqlValidator(catalogReader),
        createDefaultParserConfig(),
        createDefaultRelOptCluster());
  }

  /**
   * Converts a parsed sql statement (SqlNode) to a CalciteOperation {@link CalciteOperation}.
   *
   * @param sqlNode a parsed SQL statement
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statement
   * @param validator the {@link SqlValidator} used to validate the SQL statement. Allows for
   *     additional control of SQL functions and operators via {@link
   *     SqlValidator#getOperatorTable()}
   * @param cluster the {@link RelOptCluster} used when creating {@link RelNode}s during statement
   *     processing. Calcite expects that the {@link RelOptCluster} used during statement processing
   *     is the same as that used during query optimization.
   * @return {@link CalciteOperation} corresponding to the given SQL statement
   */
  public static CalciteOperation convertQuery(
      SqlNode sqlNode,
      Prepare.CatalogReader catalogReader,
      SqlValidator validator,
      RelOptCluster cluster) {
    return convert(sqlNode, catalogReader, validator, cluster);
  }

  /**
   * Converts a SQL statement to a CalciteOperation {@link CalciteOperation}.
   *
   * @param sqlStatement a SQL statement
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statement
   * @param validator the {@link SqlValidator} used to validate the SQL statement. Allows for
   *     additional control of SQL functions and operators via {@link
   *     SqlValidator#getOperatorTable()}
   * @param parserConfig the {@link SqlParser.Config} used for parsing the Sql statement
   * @param cluster the {@link RelOptCluster} used when creating {@link RelNode}s during statement
   *     processing. Calcite expects that the {@link RelOptCluster} used during statement processing
   *     is the same as that used during query optimization.
   * @return {@link CalciteOperation} corresponding to the given SQL statement
   * @throws SqlParseException if there is an error while parsing the SQL statement string
   */
  public static CalciteOperation convertQuery(
      String sqlStatement,
      Prepare.CatalogReader catalogReader,
      SqlValidator validator,
      SqlParser.Config parserConfig,
      RelOptCluster cluster)
      throws SqlParseException {
    List<SqlNode> sqlNodes =
        SubstraitSqlStatementParser.parseStatements(sqlStatement, parserConfig);
    if (sqlNodes.size() != 1) {
      throw new IllegalArgumentException(
          String.format("Expected one statement, found %d", sqlNodes.size()));
    }
    return convertQuery(sqlNodes.get(0), catalogReader, validator, cluster);
  }

  /**
   * Converts one or more SQL statements to a List of {@link RelRoot}, with one {@link RelRoot} per
   * statement.
   *
   * @param sqlStatements a string containing one or more SQL statements
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statements
   * @return a list of {@link RelRoot}s corresponding to the given SQL statements
   * @throws SqlParseException if there is an error while parsing the SQL statements
   */
  public static List<RelRoot> convertRelationalQueries(
      String sqlStatements, Prepare.CatalogReader catalogReader) throws SqlParseException {
    SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    return convertRelationalQueries(
        sqlStatements,
        catalogReader,
        validator,
        createDefaultParserConfig(),
        createDefaultRelOptCluster());
  }

  /**
   * Converts one or more SQL statements to a List of {@link RelRoot}, with one {@link RelRoot} per
   * statement.
   *
   * @param sqlStatements a string containing one or more SQL statements
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statements
   * @param validator the {@link SqlValidator} used to validate SQL statements. Allows for
   *     additional control of SQL functions and operators via {@link
   *     SqlValidator#getOperatorTable()}
   * @param parserConfig the {@link SqlParser.Config} used for parsing the Sql statement *
   * @param cluster the {@link RelOptCluster} used when creating {@link RelNode}s during statement
   *     processing. Calcite expects that the {@link RelOptCluster} used during statement processing
   *     is the same as that used during query optimization.
   * @return a list of {@link RelRoot}s corresponding to the given SQL statements
   * @throws SqlParseException if there is an error while parsing the SQL statements
   */
  public static List<RelRoot> convertRelationalQueries(
      String sqlStatements,
      Prepare.CatalogReader catalogReader,
      SqlValidator validator,
      SqlParser.Config parserConfig,
      RelOptCluster cluster)
      throws SqlParseException {
    List<SqlNode> sqlNodes =
        SubstraitSqlStatementParser.parseStatements(sqlStatements, parserConfig);
    return sqlNodes.stream()
        .map(sqlNode -> convert(sqlNode, catalogReader, validator, cluster))
        .filter(calciteOperation -> calciteOperation instanceof RelationalOperation)
        .map(calciteOperation -> ((RelationalOperation) calciteOperation).getRelRoot())
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Converts one or more SQL statements to a List of {@link CalciteOperation}, with one {@link
   * CalciteOperation} per statement.
   *
   * @param sqlStatements a string containing one or more SQL statements
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statements
   * @return a list of {@link CalciteOperation}s corresponding to the given SQL statements
   * @throws SqlParseException if there is an error while parsing the SQL statements
   */
  public static List<CalciteOperation> convertQueries(
      String sqlStatements, Prepare.CatalogReader catalogReader) throws SqlParseException {
    List<SqlNode> sqlNodes =
        SubstraitSqlStatementParser.parseStatements(sqlStatements, createDefaultParserConfig());
    SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    RelOptCluster cluster = createDefaultRelOptCluster();

    return sqlNodes.stream()
        .map(sqlNode -> convert(sqlNode, catalogReader, validator, cluster))
        .collect(Collectors.toUnmodifiableList());
  }
}
