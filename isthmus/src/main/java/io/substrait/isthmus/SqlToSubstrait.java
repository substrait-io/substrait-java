package io.substrait.isthmus;

import io.substrait.expression.proto.FunctionCollector;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait extends SqlConverterBase {

  public enum StatementBatching {
    SINGLE_STATEMENT,
    MULTI_STATEMENT
  }

  private final Options options;

  public SqlToSubstrait() {
    this.options = new Options();
  }

  public SqlToSubstrait(Options options) {
    this.options = options;
  }

  public static class Options {
    private final StatementBatching statementBatching;

    public Options() {
      statementBatching = StatementBatching.SINGLE_STATEMENT;
    }

    public Options(StatementBatching statementBatching) {
      this.statementBatching = statementBatching;
    }

    public StatementBatching getStatementBatching() {
      return this.statementBatching;
    }
  }

  public Plan execute(String sql, Function<List<String>, NamedStruct> tableLookup)
      throws SqlParseException {
    var pair = registerCreateTables(tableLookup);
    return executeInner(sql, factory, pair.left, pair.right);
  }

  public Plan execute(String sql, List<String> tables) throws SqlParseException {
    var pair = registerCreateTables(tables);
    return executeInner(sql, factory, pair.left, pair.right);
  }

  // Package protected for testing
  List<RelRoot> sqlToRelNode(String sql, List<String> tables) throws SqlParseException {
    var pair = registerCreateTables(tables);
    return sqlToRelNode(sql, pair.left, pair.right);
  }

  // Package protected for testing
  List<RelRoot> sqlToRelNode(String sql, Function<List<String>, NamedStruct> tableLookup)
      throws SqlParseException {
    var pair = registerCreateTables(tableLookup);
    return sqlToRelNode(sql, pair.left, pair.right);
  }

  private Plan executeInner(
      String sql,
      RelDataTypeFactory factory,
      SqlValidator validator,
      CalciteCatalogReader catalogReader)
      throws SqlParseException {
    var plan = Plan.newBuilder();
    FunctionCollector functionCollector = new FunctionCollector();
    var relProtoConverter = new RelProtoConverter(functionCollector);
    // TODO: consider case in which one sql passes conversion while others don't
    sqlToRelNode(sql, validator, catalogReader)
        .forEach(
            root -> {
              plan.addRelations(
                  PlanRel.newBuilder()
                      .setRoot(
                          io.substrait.proto.RelRoot.newBuilder()
                              .setInput(
                                  SubstraitRelVisitor.convert(root, EXTENSION_COLLECTION)
                                      .accept(relProtoConverter))
                              .addAllNames(
                                  TypeConverter.toNamedStruct(root.validatedRowType).names())));
            });
    functionCollector.addFunctionsToPlan(plan);
    return plan.build();
  }

  private List<RelRoot> sqlToRelNode(
      String sql, SqlValidator validator, CalciteCatalogReader catalogReader)
      throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig);
    var parsedList = parser.parseStmtList();
    if (options.getStatementBatching() == StatementBatching.SINGLE_STATEMENT
        && parsedList.size() > 1) {
      throw new UnsupportedOperationException("SQL must contain only a single statement: " + sql);
    }
    SqlToRelConverter converter =
        new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            relOptCluster,
            StandardConvertletTable.INSTANCE,
            converterConfig);
    List<RelRoot> roots =
        parsedList.stream()
            .map(
                parsed -> {
                  RelRoot root = converter.convertQuery(parsed, true, true);
                  {
                    var program = HepProgram.builder().build();
                    HepPlanner hepPlanner = new HepPlanner(program);
                    hepPlanner.setRoot(root.rel);
                    root = root.withRel(hepPlanner.findBestExp());
                  }
                  return root;
                })
            .toList();
    return roots;
  }
}
