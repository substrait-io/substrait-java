package io.substrait.isthmus;

import io.substrait.expression.proto.FunctionCollector;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.LookupCalciteSchema;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);

    return executeInner(sql, factory, validator, catalogReader);
  }

  public Plan execute(String sql, List<String> tables) throws SqlParseException {
    var pair = registerCreateTables(tables);
    return executeInner(sql, factory, pair.left, pair.right);
  }

  public RelRoot sqlToRelNode(String sql, List<String> tables) throws SqlParseException {
    var pair = registerCreateTables(tables);
    return sqlToRelNode(sql, factory, pair.left, pair.right);
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

    SqlToRelConverter converter =
        new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            relOptCluster,
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
