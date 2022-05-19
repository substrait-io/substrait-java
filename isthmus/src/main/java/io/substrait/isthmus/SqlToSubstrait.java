package io.substrait.isthmus;

import io.substrait.expression.proto.FunctionCollector;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.jdbc.LookupCalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.ProxyingMetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait extends SqlConverterBase {

  public Plan execute(String sql, Function<List<String>, NamedStruct> tableLookup)
      throws SqlParseException {
    RelDataTypeFactory factory = new JavaTypeFactoryImpl();
    Function<List<String>, Table> lookup =
        id -> {
          NamedStruct table = tableLookup.apply(id);
          if (table == null) {
            return null;
          }
          return new DefinedTable(
              id.get(id.size() - 1),
              factory,
              TypeConverter.convert(factory, table.struct(), table.names()));
        };

    CalciteSchema rootSchema = LookupCalciteSchema.createRootSchema(lookup);
    CalciteConnectionConfig config =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);

    return executeInner(sql, factory, validator, catalogReader);
  }

  public Plan execute(String sql, List<String> tables) throws SqlParseException {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    RelDataTypeFactory factory = new JavaTypeFactoryImpl();
    CalciteConnectionConfig config =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
    if (tables != null) {
      for (String tableDef : tables) {
        List<DefinedTable> tList = parseCreateTable(factory, validator, tableDef);
        for (DefinedTable t : tList) {
          rootSchema.add(t.getName(), t);
        }
      }
    }

    return executeInner(sql, factory, validator, catalogReader);
  }

  public RelRoot sqlToRelNode(String sql, List<String> tables) throws SqlParseException {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    RelDataTypeFactory factory = new JavaTypeFactoryImpl();
    CalciteConnectionConfig config =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
    if (tables != null) {
      for (String tableDef : tables) {
        List<DefinedTable> tList = parseCreateTable(factory, validator, tableDef);
        for (DefinedTable t : tList) {
          rootSchema.add(t.getName(), t);
        }
      }
    }
    return sqlToRelNode(sql, factory, validator, catalogReader);
  }

  private Plan executeInner(
      String sql,
      RelDataTypeFactory factory,
      SqlValidator validator,
      CalciteCatalogReader catalogReader)
      throws SqlParseException {
    RelRoot root = sqlToRelNode(sql, factory, validator, catalogReader);

    // System.out.println(RelOptUtil.toString(root.rel));
    Rel pojoRel = SubstraitRelVisitor.convert(root, EXTENSION_COLLECTION);
    FunctionCollector functionCollector = new FunctionCollector();
    RelProtoConverter toProtoRel = new RelProtoConverter(functionCollector);
    var protoRel = pojoRel.accept(toProtoRel);

    var planRel =
        PlanRel.newBuilder()
            .setRoot(
                io.substrait.proto.RelRoot.newBuilder()
                    .setInput(protoRel)
                    .addAllNames(TypeConverter.toNamedStruct(root.validatedRowType).names()));

    var plan = Plan.newBuilder();
    plan.addRelations(planRel);
    functionCollector.addFunctionsToPlan(plan);
    return plan.build();
  }

  private RelRoot sqlToRelNode(
      String sql,
      RelDataTypeFactory factory,
      SqlValidator validator,
      CalciteCatalogReader catalogReader)
      throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
    var parsed = parser.parseQuery();
    SqlToRelConverter.Config converterConfig =
        SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);

    VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of("hello"));
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(factory));

    cluster.setMetadataQuerySupplier(
        () -> {
          ProxyingMetadataHandlerProvider handler =
              new ProxyingMetadataHandlerProvider(DefaultRelMetadataProvider.INSTANCE);
          return new RelMetadataQuery(handler);
        });

    SqlToRelConverter converter =
        new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig);
    RelRoot root = converter.convertQuery(parsed, true, true);
    {
      var program = HepProgram.builder().build();
      HepPlanner hepPlanner = new HepPlanner(program);
      hepPlanner.setRoot(root.rel);
      root = root.withRel(hepPlanner.findBestExp());
    }
    return root;
  }
}
