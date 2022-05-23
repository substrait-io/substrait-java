package io.substrait.isthmus;

import com.google.common.collect.Lists;
import io.substrait.expression.proto.FunctionCollector;
import io.substrait.function.ImmutableSimpleExtension;
import io.substrait.function.SimpleExtension;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait {

  public Plan execute(String sql, Function<List<String>, Map<String, Type>> tableLookup)
      throws SqlParseException {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    RelDataTypeFactory factory = new JavaTypeFactoryImpl();
    CalciteConnectionConfig config =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
    SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
    var parsed = parser.parseQuery();
    Set<SqlIdentifier> ids = new HashSet<>();

    parsed.accept(
        new SqlBasicVisitor<>() {
          @Override
          public Object visit(SqlIdentifier id) {
            ids.add(id);
            return super.visit(id);
          }
        });

    for (SqlIdentifier id : ids) {
      Map<String, Type> table = tableLookup.apply(id.names);
      if (table == null) {
        continue;
      }
      List<RelDataType> types = Lists.newArrayList();
      List<String> names = Lists.newArrayList();
      table.forEach(
          (k, v) -> {
            names.add(k);
            types.add(TypeConverter.convert(factory, v));
          });
      DefinedTable dt =
          new DefinedTable(id.getSimple(), factory, factory.createStructType(types, names));
      rootSchema.add(dt.getName(), dt);
    }

    return executeInner(parsed, validator, factory, catalogReader);
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

    SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
    var parsed = parser.parseQuery();
    return executeInner(parsed, validator, factory, catalogReader);
  }

  private Plan executeInner(
      SqlNode parsed,
      SqlValidator validator,
      RelDataTypeFactory factory,
      CalciteCatalogReader catalogReader) {
    SqlToRelConverter.Config converterConfig =
        SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);
    validator.validate(parsed);

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

  private static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION;

  static {
    SimpleExtension.ExtensionCollection defaults =
        ImmutableSimpleExtension.ExtensionCollection.builder().build();
    try {
      defaults = SimpleExtension.loadDefaults();
    } catch (IOException e) {
      throw new RuntimeException("Failure while loading defaults.", e);
    }

    EXTENSION_COLLECTION = defaults;
  }

  private List<DefinedTable> parseCreateTable(
      RelDataTypeFactory factory, SqlValidator validator, String sql) throws SqlParseException {
    SqlParser parser =
        SqlParser.create(sql, SqlParser.Config.DEFAULT.withParserFactory(SqlDdlParserImpl.FACTORY));
    List<DefinedTable> definedTableList = new ArrayList<>();

    SqlNodeList nodeList = parser.parseStmtList();
    for (SqlNode parsed : nodeList) {
      if (!(parsed instanceof SqlCreateTable)) {
        fail("Not a valid CREATE TABLE statement.");
      }

      SqlCreateTable create = (SqlCreateTable) parsed;
      if (create.name.names.size() > 1) {
        fail("Only simple table names are allowed.", create.name.getParserPosition());
      }

      if (create.query != null) {
        fail("CTAS not supported.", create.name.getParserPosition());
      }

      List<String> names = new ArrayList<>();
      List<RelDataType> columnTypes = new ArrayList<>();

      for (SqlNode node : create.columnList) {
        if (!(node instanceof SqlColumnDeclaration)) {
          fail("Unexpected column list construction.", node.getParserPosition());
        }

        SqlColumnDeclaration col = (SqlColumnDeclaration) node;
        if (col.name.names.size() != 1) {
          fail("Expected simple column names.", col.name.getParserPosition());
        }

        names.add(col.name.names.get(0));
        columnTypes.add(col.dataType.deriveType(validator));
      }

      definedTableList.add(
          new DefinedTable(
              create.name.names.get(0), factory, factory.createStructType(columnTypes, names)));
    }

    return definedTableList;
  }

  private static SqlParseException fail(String text, SqlParserPos pos) {
    return new SqlParseException(text, pos, null, null, new RuntimeException("fake lineage"));
  }

  private static SqlParseException fail(String text) {
    return fail(text, SqlParserPos.ZERO);
  }

  private static final class Validator extends SqlValidatorImpl {

    private Validator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        Config config) {
      super(opTab, catalogReader, typeFactory, config);
    }

    public static Validator create(
        RelDataTypeFactory factory, CalciteCatalogReader catalog, SqlValidator.Config config) {
      return new Validator(SqlStdOperatorTable.instance(), catalog, factory, config);
    }
  }

  /** A fully defined pre-specified table. */
  private static final class DefinedTable extends AbstractTable {

    private final String name;
    private final RelDataTypeFactory factory;
    private final RelDataType type;

    public DefinedTable(String name, RelDataTypeFactory factory, RelDataType type) {
      this.name = name;
      this.factory = factory;
      this.type = type;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      if (factory != typeFactory) {
        throw new IllegalStateException("Different type factory than previously used.");
      }
      return type;
    }

    public String getName() {
      return name;
    }
  }
}
