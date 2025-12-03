package io.substrait.isthmus;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import org.apache.calcite.prepare.Prepare;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KeyConstraintsTest extends PlanTestBase {

  @ParameterizedTest
  @ValueSource(ints = {7})
  void tpcds(final int query) throws Exception {
    final SqlToSubstrait s = new SqlToSubstrait();
    final String values = asString("keyconstraints_schema.sql");
    final Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(values);
    s.convert(asString(String.format("tpcds/queries/%02d.sql", query)), catalog);
  }
}
