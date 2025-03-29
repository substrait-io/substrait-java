package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitTable;
import io.substrait.isthmus.sql.SubstraitSqlValidator;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.ProxyingMetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
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
import org.apache.calcite.sql2rel.SqlToRelConverter;

class SqlConverterBase {
  final RelDataTypeFactory factory;
  final RelOptCluster relOptCluster;
  final CalciteConnectionConfig config;
  final SqlToRelConverter.Config converterConfig;

  final SqlParser.Config parserConfig;

  protected static final FeatureBoard FEATURES_DEFAULT = ImmutableFeatureBoard.builder().build();
  final FeatureBoard featureBoard;

  protected SqlConverterBase(FeatureBoard features) {
    this.factory = new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM);
    this.config =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    this.converterConfig = SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);
    VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of("hello"));
    this.relOptCluster = RelOptCluster.create(planner, new RexBuilder(factory));
    relOptCluster.setMetadataQuerySupplier(
        () -> {
          ProxyingMetadataHandlerProvider handler =
              new ProxyingMetadataHandlerProvider(DefaultRelMetadataProvider.INSTANCE);
          return new RelMetadataQuery(handler);
        });
    featureBoard = features == null ? FEATURES_DEFAULT : features;
    parserConfig =
        SqlParser.Config.DEFAULT
            .withUnquotedCasing(featureBoard.unquotedCasing())
            .withParserFactory(SqlDdlParserImpl.FACTORY)
            .withConformance(SqlConformanceEnum.LENIENT);
  }

  protected static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION =
      SimpleExtension.loadDefaults();

  CalciteCatalogReader registerCreateTables(List<String> tables) throws SqlParseException {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    if (tables != null) {
      for (String tableDef : tables) {
        List<SubstraitTable> tList = parseCreateTable(factory, validator, tableDef);
        for (SubstraitTable t : tList) {
          rootSchema.add(t.getName(), t);
        }
      }
    }
    return catalogReader;
  }

  CalciteCatalogReader registerSchema(String name, Schema schema) {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    if (schema != null) {
      rootSchema.add(name, schema);
      rootSchema = rootSchema.getSubSchema(name, false);
    }
    return new CalciteCatalogReader(rootSchema, List.of(), factory, config);
  }

  protected List<SubstraitTable> parseCreateTable(
      RelDataTypeFactory factory, SqlValidator validator, String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig);
    List<SubstraitTable> tableList = new ArrayList<>();

    SqlNodeList nodeList = parser.parseStmtList();
    for (SqlNode parsed : nodeList) {
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

      List<String> names = new ArrayList<>();
      List<RelDataType> columnTypes = new ArrayList<>();

      for (SqlNode node : create.columnList) {
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
        columnTypes.add(col.dataType.deriveType(validator));
      }

      tableList.add(
          new SubstraitTable(
              create.name.names.get(0), factory.createStructType(columnTypes, names)));
    }

    return tableList;
  }

  protected static SqlParseException fail(String text, SqlParserPos pos) {
    return new SqlParseException(text, pos, null, null, new RuntimeException("fake lineage"));
  }

  protected static SqlParseException fail(String text) {
    return fail(text, SqlParserPos.ZERO);
  }
}
