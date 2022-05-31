package io.substrait.isthmus;

import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.LookupCalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidator;

public class SubstraitToSql extends SqlConverterBase {
  protected final CalciteCatalogReader catalogReader;
  protected final SqlValidator validator;

  public SubstraitToSql() {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    this.catalogReader = new CalciteCatalogReader(rootSchema, Arrays.asList(), factory, config);
    this.validator =
        SqlToSubstrait.Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
  }

  public RelNode substraitRelToCalciteRel(Rel relRoot, List<String> tables)
      throws SqlParseException {
    if (tables != null) {
      for (String tableDef : tables) {
        List<SqlToSubstrait.DefinedTable> tList = parseCreateTable(factory, validator, tableDef);
        for (SqlToSubstrait.DefinedTable t : tList) {
          catalogReader.getRootSchema().add(t.getName(), t);
        }
      }
    }

    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, catalogReader, parserConfig);
  }

  public RelNode substraitRelToCalciteRel(
      Rel relRoot, Function<List<String>, NamedStruct> tableLookup) throws SqlParseException {
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

    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, catalogReader, parserConfig);
  }

  private static final SqlDialect DEFAULT_DIALECT =
      new SqlDialect(
          SqlDialect.EMPTY_CONTEXT
              .withDatabaseProduct(SqlDialect.DatabaseProduct.SNOWFLAKE)
              .withDatabaseProductName("SNOWFLAKE")
              .withIdentifierQuoteString(null)
              .withNullCollation(NullCollation.HIGH));

  public static String toSql(RelNode root) {
    return toSql(root, DEFAULT_DIALECT);
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
