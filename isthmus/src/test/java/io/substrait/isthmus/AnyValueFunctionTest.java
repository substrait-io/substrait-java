package io.substrait.isthmus;

import org.junit.jupiter.api.Test;

class AnyValueFunctionTest extends PlanTestBase {

  AnyValueFunctionTest() {
    super(new AutomaticDynamicFunctionMappingConverterProvider());
  }

  @Test
  void simpleAnyValue() throws Exception {
    String query = "SELECT any_value(l_orderkey) FROM lineitem";
    assertFullRoundTrip(query);
  }

  @Test
  void windowFunctionRowNumber() throws Exception {
    String query =
        "SELECT l_orderkey, ROW_NUMBER() OVER (PARTITION BY l_suppkey ORDER BY l_orderkey) as rn FROM lineitem";
    assertFullRoundTrip(query);
  }

  @Test
  void windowFunctionLag() throws Exception {
    String query =
        "SELECT l_orderkey, LAG(l_quantity) OVER (PARTITION BY l_suppkey ORDER BY l_orderkey) as prev_qty FROM lineitem";
    assertFullRoundTrip(query);
  }

  @Test
  void windowFunctionFirstValue() throws Exception {
    String query =
        "SELECT l_orderkey, FIRST_VALUE(l_quantity) OVER (PARTITION BY l_suppkey ORDER BY l_orderkey) as first_qty FROM lineitem";
    assertFullRoundTrip(query);
  }
}
