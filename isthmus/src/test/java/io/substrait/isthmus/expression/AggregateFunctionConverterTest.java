package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.*;

import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.TypeConverter;
import java.util.Arrays;
import java.util.Collections;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

public class AggregateFunctionConverterTest extends PlanTestBase {

  @Test
  void testFunctionFinderMatch() {
    AggregateFunctionConverter converter =
        new AggregateFunctionConverter(
            extensions.aggregateFunctions(),
            Collections.emptyList(),
            typeFactory,
            TypeConverter.DEFAULT);

    var functionFinder =
        converter.getFunctionFinder(
            AggregateCall.create(
                new SqlSumEmptyIsZeroAggFunction(),
                true,
                Arrays.asList(1),
                0,
                typeFactory.createSqlType(SqlTypeName.VARCHAR),
                null));
    assertNotNull(functionFinder);
    assertEquals("sum0", functionFinder.getSubstraitName());
    assertEquals(AggregateFunctions.SUM0, functionFinder.getOperator());
  }
}
