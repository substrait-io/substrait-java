package io.substrait.isthmus;

import io.substrait.relation.Rel;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class SubstraitToSql extends SqlConverterBase {
  private final RelDataTypeFactory factory;
  private final CalciteCatalogReader catalogReader;
  private final RelOptCluster relOptCluster;
  private final SqlValidator validator;

  SubstraitToSql() {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    SqlToRelConverter.Config converterConfig =
        SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);
    this.factory = new JavaTypeFactoryImpl();

    CalciteConnectionConfig config =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    this.catalogReader = new CalciteCatalogReader(rootSchema, Arrays.asList(), factory, config);

    SqlValidator validator =
        SqlToSubstrait.Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);

    VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of("hello"));
    this.relOptCluster = RelOptCluster.create(planner, new RexBuilder(factory));
    this.validator =
        SqlToSubstrait.Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
  }

  public RelNode convert(Rel relRoot, List<String> tables) throws SqlParseException {
    if (tables != null) {
      for (String tableDef : tables) {
        List<SqlToSubstrait.DefinedTable> tList = parseCreateTable(factory, validator, tableDef);
        for (SqlToSubstrait.DefinedTable t : tList) {
          catalogReader.getRootSchema().add(t.getName(), t);
        }
      }
    }

    VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of("hello"));
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(factory));

    return SubstraitRelNodeConverter.convert(relRoot, cluster, catalogReader);
  }

  public static String toSql(RelNode root, SqlDialect dialect) {
    return toSql(
        root,
        dialect,
        c ->
            c.withAlwaysUseParentheses(false)
                .withSelectListItemsOnSeparateLines(false)
                .withUpdateSetListNewline(false)
                .withIndentation(0));
  }

  private static String toSql(
      RelNode root, SqlDialect dialect, UnaryOperator<SqlWriterConfig> transform) {
    final RelToSqlConverter converter = new RelToSqlConverter(dialect);
    final SqlNode sqlNode = converter.visitRoot(root).asStatement();
    return sqlNode.toSqlString(c -> transform.apply(c.withDialect(dialect))).getSql();
  }
}
