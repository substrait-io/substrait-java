package io.substrait.isthmus;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import org.apache.calcite.prepare.Prepare;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class KeyConstraintsTest extends PlanTestBase {

  @ParameterizedTest
  @ValueSource(ints = {7})
  public void tpcds(int query) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    String values = asString("keyconstraints_schema.sql");
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(values);
    s.convert(asString(String.format("tpcds/queries/%02d.sql", query)), catalog);
  }
}
