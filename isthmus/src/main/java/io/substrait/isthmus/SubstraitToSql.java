package io.substrait.isthmus;

import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParseException;

import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;

public class SubstraitToSql extends SqlConverterBase {

  public SubstraitToSql() {
    super(FEATURES_DEFAULT);
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
  }

  public RelNode substraitRelToCalciteRel(Rel relRoot, List<String> tables)
      throws SqlParseException {
    var pair = registerCreateTables(tables);
    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, pair.right, parserConfig);
  }

  public RelNode substraitRelToCalciteRel(
      Rel relRoot, Function<List<String>, NamedStruct> tableLookup) throws SqlParseException {
    var pair = registerCreateTables(tableLookup);

    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, pair.right, parserConfig);
  }

  // DEFAULT_SQL_DIALECT uses Calcite's EMPTY_CONTEXT with setting:
  // identifierQuoteString : null, identifierEscapeQuoteString : null
  // quotedCasing : UNCHANGED, unquotedCasing : TO_UPPER
  // caseSensitive: true
  // supportsApproxCountDistinct is true
  private static final SqlDialect DEFAULT_SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT) {
    @Override
    public boolean supportsApproxCountDistinct() {
      return true;
    }
  };

  public static String toSql(RelNode root) {
    return toSql(root, DEFAULT_SQL_DIALECT);
  }

  public static List<String> toSql(Plan plan) {
    return toSql(plan, DEFAULT_SQL_DIALECT);
  }

  public static List<String> toSql(Plan plan, SqlDialect dialect) {
    SimpleExtension.ExtensionCollection extensions = SimpleExtension.loadDefaults();

    SubstraitToCalcite substrait2Calcite = new SubstraitToCalcite(
        extensions, new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM));

    return plan.getRoots().stream().map(root -> {
      var calciteRel = substrait2Calcite.convert(root).project();
      return SubstraitToSql.toSql(calciteRel);
    }).collect(Collectors.toList());

  }

  public static String toSql(RelNode root, SqlDialect dialect) {
    return toSql(
        root,
        dialect,
        c -> c.withAlwaysUseParentheses(false)
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
