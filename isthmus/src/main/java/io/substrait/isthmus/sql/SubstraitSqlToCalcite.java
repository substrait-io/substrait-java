package io.substrait.isthmus.sql;

import io.substrait.isthmus.SubstraitTypeSystem;
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
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public class SubstraitSqlToCalcite {

  public static RelRoot convertSelect(String selectStatement, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    return convertSelect(selectStatement, catalogReader, createRelOptCluster());
  }

  public static RelRoot convertSelect(
      String selectStatement, Prepare.CatalogReader catalogReader, RelOptCluster cluster)
      throws SqlParseException {
    List<SqlNode> sqlNodes = SubstraitSelectStatementParser.parseSelectStatements(selectStatement);
    if (sqlNodes.size() != 1) {
      throw new IllegalArgumentException(
          String.format("Expected one SELECT statement, found: %d", sqlNodes.size()));
    }
    List<RelRoot> relRoots = convert(sqlNodes, catalogReader, cluster);
    // as there was only 1 select statement, there should only be 1 root
    return relRoots.get(0);
  }

  public static List<RelRoot> convertSelects(
      String selectStatements, Prepare.CatalogReader catalogReader) throws SqlParseException {
    return convertSelects(selectStatements, catalogReader, createRelOptCluster());
  }

  public static List<RelRoot> convertSelects(
      String selectStatements, Prepare.CatalogReader catalogReader, RelOptCluster cluster)
      throws SqlParseException {
    List<SqlNode> sqlNodes = SubstraitSelectStatementParser.parseSelectStatements(selectStatements);
    return convert(sqlNodes, catalogReader, cluster);
  }

  static List<RelRoot> convert(
      List<SqlNode> selectStatements, Prepare.CatalogReader catalogReader, RelOptCluster cluster) {
    RelOptTable.ViewExpander viewExpander = null;
    SqlToRelConverter converter =
        new SqlToRelConverter(
            viewExpander,
            new SubstraitSqlValidator(catalogReader),
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.CONFIG);
    // apply validation
    boolean needsValidation = true;
    // query is the root of the tree
    boolean top = true;
    return selectStatements.stream()
        .map(
            sqlNode -> removeUnnecessaryProjects(converter.convertQuery(sqlNode, needsValidation, top)))
        .collect(Collectors.toList());
  }

  static RelOptCluster createRelOptCluster() {
    RexBuilder rexBuilder =
        new RexBuilder(new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM));
    HepProgram program = HepProgram.builder().build();
    RelOptPlanner emptyPlanner = new HepPlanner(program);
    return RelOptCluster.create(emptyPlanner, rexBuilder);
  }

  static RelRoot removeUnnecessaryProjects(RelRoot root) {
    return root.withRel(removeUnnecessaryProjects(root.rel));
  }

  static RelNode removeUnnecessaryProjects(RelNode root) {
    HepProgram program = HepProgram.builder().addRuleInstance(CoreRules.PROJECT_REMOVE).build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(root);
    return planner.findBestExp();
  }
}
