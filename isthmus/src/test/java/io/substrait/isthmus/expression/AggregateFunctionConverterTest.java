package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.FunctionConverter.FunctionFinder;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class AggregateFunctionConverterTest extends PlanTestBase {

  @Test
  void testFunctionFinderMatch() {
    AggregateFunctionConverter converter =
        new AggregateFunctionConverter(
            extensions.aggregateFunctions(), List.of(), typeFactory, TypeConverter.DEFAULT);

    FunctionFinder functionFinder =
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

  @Test
  void testStddevPopFunctionFinderMatch() {
    AggregateFunctionConverter converter =
        new AggregateFunctionConverter(
            extensions.aggregateFunctions(), List.of(), typeFactory, TypeConverter.DEFAULT);

    FunctionFinder functionFinder =
        converter.getFunctionFinder(
            AggregateCall.create(
                new SqlAvgAggFunction(SqlKind.STDDEV_POP),
                false,
                List.of(0),
                -1,
                typeFactory.createSqlType(SqlTypeName.DOUBLE),
                null));
    assertNotNull(functionFinder);
    assertEquals("std_dev", functionFinder.getSubstraitName());
    assertEquals(AggregateFunctions.STDDEV_POP, functionFinder.getOperator());
  }

  @Test
  void testStddevSampFunctionFinderMatch() {
    AggregateFunctionConverter converter =
        new AggregateFunctionConverter(
            extensions.aggregateFunctions(), List.of(), typeFactory, TypeConverter.DEFAULT);

    FunctionFinder functionFinder =
        converter.getFunctionFinder(
            AggregateCall.create(
                new SqlAvgAggFunction(SqlKind.STDDEV_SAMP),
                false,
                List.of(0),
                -1,
                typeFactory.createSqlType(SqlTypeName.DOUBLE),
                null));
    assertNotNull(functionFinder);
    assertEquals("std_dev", functionFinder.getSubstraitName());
    assertEquals(AggregateFunctions.STDDEV_SAMP, functionFinder.getOperator());
  }

  @Test
  void testVarPopFunctionFinderMatch() {
    AggregateFunctionConverter converter =
        new AggregateFunctionConverter(
            extensions.aggregateFunctions(), List.of(), typeFactory, TypeConverter.DEFAULT);

    FunctionFinder functionFinder =
        converter.getFunctionFinder(
            AggregateCall.create(
                new SqlAvgAggFunction(SqlKind.VAR_POP),
                false,
                List.of(0),
                -1,
                typeFactory.createSqlType(SqlTypeName.DOUBLE),
                null));
    assertNotNull(functionFinder);
    assertEquals("variance", functionFinder.getSubstraitName());
    assertEquals(AggregateFunctions.VAR_POP, functionFinder.getOperator());
  }

  @Test
  void testVarSampFunctionFinderMatch() {
    AggregateFunctionConverter converter =
        new AggregateFunctionConverter(
            extensions.aggregateFunctions(), List.of(), typeFactory, TypeConverter.DEFAULT);

    FunctionFinder functionFinder =
        converter.getFunctionFinder(
            AggregateCall.create(
                new SqlAvgAggFunction(SqlKind.VAR_SAMP),
                false,
                List.of(0),
                -1,
                typeFactory.createSqlType(SqlTypeName.DOUBLE),
                null));
    assertNotNull(functionFinder);
    assertEquals("variance", functionFinder.getSubstraitName());
    assertEquals(AggregateFunctions.VAR_SAMP, functionFinder.getOperator());
  }
}
