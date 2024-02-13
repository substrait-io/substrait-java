package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.*;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.RelCreator;
import io.substrait.isthmus.TypeConverter;
import java.io.IOException;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

public class AggregateFunctionConverterTest {

  protected static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION;

  static {
    SimpleExtension.ExtensionCollection defaults;
    try {
      defaults = SimpleExtension.loadDefaults();
    } catch (IOException e) {
      throw new RuntimeException("Failure while loading defaults.", e);
    }

    EXTENSION_COLLECTION = defaults;
  }

  final SubstraitBuilder b = new SubstraitBuilder(EXTENSION_COLLECTION);

  @Test
  public void testFunctionFinderMatch() {

    RelCreator relCreator = new RelCreator();
    RelDataTypeFactory typeFactory = relCreator.typeFactory();

    AggregateFunctionConverter converter =
        new AggregateFunctionConverter(
            EXTENSION_COLLECTION.aggregateFunctions(),
            List.of(),
            typeFactory,
            TypeConverter.DEFAULT);

    var functionFinder =
        converter.getFunctionFinder(
            AggregateCall.create(
                new SqlSumEmptyIsZeroAggFunction(),
                true,
                List.of(1),
                0,
                typeFactory.createSqlType(SqlTypeName.VARCHAR),
                null));
    assertNotNull(functionFinder);
    assertEquals("sum0", functionFinder.getName());
  }
}
