package io.substrait.isthmus.sql;

import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.calcite.rel.DdlSqlToRelConverter;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/**
 * Substrait flavoured SQL processor provided as a utility for testing and experimentation,
 * utilizing {@link SubstraitSqlStatementParser} and {@link SubstraitSqlValidator}
 */
public class SubstraitSqlToCalcite {

  /**
   * Converts a SQL statement to a Calcite {@link RelRoot}.
   *
   * @param sqlStatement a SQL statement string
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statement
   * @return a {@link RelRoot} corresponding to the given SQL statement
   * @throws SqlParseException if there is an error while parsing the SQL statement
   */
  public static RelRoot convertQuery(
      final String sqlStatement, final Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    final SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    return convertQuery(sqlStatement, catalogReader, validator, createDefaultRelOptCluster());
  }

  /**
   * Converts a SQL statement to a Calcite {@link RelRoot}.
   *
   * @param sqlStatement a SQL statement
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statement
   * @param validator the {@link SqlValidator} used to validate the SQL statement. Allows for
   *     additional control of SQL functions and operators via {@link
   *     SqlValidator#getOperatorTable()}
   * @param cluster the {@link RelOptCluster} used when creating {@link RelNode}s during statement
   *     processing. Calcite expects that the {@link RelOptCluster} used during statement processing
   *     is the same as that used during query optimization.
   * @return {@link RelRoot} corresponding to the given SQL statement
   * @throws SqlParseException if there is an error while parsing the SQL statement string
   */
  public static RelRoot convertQuery(
      final String sqlStatement,
      final Prepare.CatalogReader catalogReader,
      final SqlValidator validator,
      final RelOptCluster cluster)
      throws SqlParseException {
    final List<SqlNode> sqlNodes = SubstraitSqlStatementParser.parseStatements(sqlStatement);
    if (sqlNodes.size() != 1) {
      throw new IllegalArgumentException(
          String.format("Expected one statement, found: %d", sqlNodes.size()));
    }
    final List<RelRoot> relRoots = convert(sqlNodes, catalogReader, validator, cluster);
    // as there was only 1 statement, there should only be 1 root
    return relRoots.get(0);
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
  public static List<RelRoot> convertQueries(
      final String sqlStatements, final Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    final SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    return convertQueries(sqlStatements, catalogReader, validator, createDefaultRelOptCluster());
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
   * @param cluster the {@link RelOptCluster} used when creating {@link RelNode}s during statement
   *     processing. Calcite expects that the {@link RelOptCluster} used during statement processing
   *     is the same as that used during query optimization.
   * @return a list of {@link RelRoot}s corresponding to the given SQL statements
   * @throws SqlParseException if there is an error while parsing the SQL statements
   */
  public static List<RelRoot> convertQueries(
      final String sqlStatements,
      final Prepare.CatalogReader catalogReader,
      final SqlValidator validator,
      final RelOptCluster cluster)
      throws SqlParseException {
    final List<SqlNode> sqlNodes = SubstraitSqlStatementParser.parseStatements(sqlStatements);
    return convert(sqlNodes, catalogReader, validator, cluster);
  }

  static List<RelRoot> convert(
      final List<SqlNode> sqlNodes,
      final Prepare.CatalogReader catalogReader,
      final SqlValidator validator,
      final RelOptCluster cluster) {
    final RelOptTable.ViewExpander viewExpander = null;
    final SqlToRelConverter converter =
        new SqlToRelConverter(
            viewExpander,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.CONFIG);
    final DdlSqlToRelConverter ddlSqlToRelConverter = new DdlSqlToRelConverter(converter);
    return sqlNodes.stream()
        .map(sqlNode -> sqlNode.accept(ddlSqlToRelConverter))
        .collect(Collectors.toList());
  }

  static RelOptCluster createDefaultRelOptCluster() {
    final RexBuilder rexBuilder =
        new RexBuilder(new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM));
    final HepProgram program = HepProgram.builder().build();
    final RelOptPlanner emptyPlanner = new HepPlanner(program);
    return RelOptCluster.create(emptyPlanner, rexBuilder);
  }
}
