package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.FunctionConverter.FunctionFinder;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class AggregateFunctionConverterTest extends PlanTestBase {

  @Test
  void testFunctionFinderMatch() {
    final AggregateFunctionConverter converter =
        new AggregateFunctionConverter(
            extensions.aggregateFunctions(), List.of(), typeFactory, TypeConverter.DEFAULT);

    final FunctionFinder functionFinder =
        converter.getFunctionFinder(
            AggregateCall.create(
                new SqlSumEmptyIsZeroAggFunction(),
                true,
                List.of(1),
                0,
                typeFactory.createSqlType(SqlTypeName.VARCHAR),
                null));
    assertNotNull(functionFinder);
    assertEquals("sum0", functionFinder.getSubstraitName());
    assertEquals(AggregateFunctions.SUM0, functionFinder.getOperator());
  }
}
