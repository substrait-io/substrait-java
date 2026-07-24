package io.substrait.isthmus.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.isthmus.AutomaticDynamicFunctionMappingConverterProvider;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.UserTypeMapper;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.type.Type;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

/**
 * Backs the runnable code samples in {@code docs/isthmus/customization.md}. Regions marked with
 * {@code // --8<-- [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--}
 * snippet includes.
 */
class CustomizationDocTest extends PlanTestBase {

  @Test
  void customSqlParserConfig() throws Exception {
    String sql = "SELECT x FROM t";
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE t(x VARCHAR NOT NULL)");
    // --8<-- [start:parser-config]
    ConverterProvider provider =
        new ConverterProvider() {
          @Override
          public SqlParser.Config getSqlParserConfig() {
            return SqlParser.Config.DEFAULT
                .withUnquotedCasing(Casing.UNCHANGED)
                .withParserFactory(SqlDdlParserImpl.FACTORY)
                .withConformance(SqlConformanceEnum.LENIENT);
          }
        };

    Plan plan = new SqlToSubstrait(provider).convert(sql, catalog);
    // --8<-- [end:parser-config]
    assertNotNull(plan);
  }

  @Test
  void userTypeMapper() {
    UserTypeMapper myUserTypeMapper =
        new UserTypeMapper() {
          @Override
          public Type toSubstrait(RelDataType relDataType) {
            return null;
          }

          @Override
          public RelDataType toCalcite(Type.UserDefined type) {
            return null;
          }
        };
    // --8<-- [start:type-converter]
    TypeConverter typeConverter = new TypeConverter(myUserTypeMapper);
    // --8<-- [end:type-converter]
    assertNotNull(typeConverter);
  }

  @Test
  void automaticDynamicFunctionMapping() {
    // --8<-- [start:automatic]
    SqlToSubstrait converter =
        new SqlToSubstrait(new AutomaticDynamicFunctionMappingConverterProvider());
    // --8<-- [end:automatic]
    assertNotNull(converter);
  }
}
