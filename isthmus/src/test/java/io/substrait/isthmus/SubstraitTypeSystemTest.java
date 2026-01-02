package io.substrait.isthmus;

import static io.substrait.isthmus.SubstraitTypeSystem.TYPE_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class SubstraitTypeSystemTest {

  private final RelDataTypeSystem typeSystem = SubstraitTypeSystem.TYPE_SYSTEM;

  @Test
  void decimalMaxPrecision() {
    assertEquals(38, typeSystem.getMaxPrecision(SqlTypeName.DECIMAL));
  }

  @Test
  void decimalMaxScale() {
    assertEquals(38, typeSystem.getMaxScale(SqlTypeName.DECIMAL));
  }

  @Test
  void decimalDefaultPrecision() {
    assertEquals(38, typeSystem.getDefaultPrecision(SqlTypeName.DECIMAL));
  }

  @Test
  void decimalDefaultScale() {
    assertEquals(0, typeSystem.getDefaultScale(SqlTypeName.DECIMAL));
  }

  @Test
  void timestampMaxPrecision() {
    assertEquals(6, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP));
  }

  @Test
  void timeMaxPrecision() {
    assertEquals(6, typeSystem.getMaxPrecision(SqlTypeName.TIME));
  }

  @Test
  void canCreateDecimalWithMaxPrecision() {
    RelDataType decimalType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 38, 10);
    assertEquals(38, decimalType.getPrecision());
    assertEquals(10, decimalType.getScale());
  }

  @Test
  void decimalMaxPrecisionAndScaleDifferentFromDefaultTypeSystem() {
    RelDataTypeSystem defaultTypeSystem = RelDataTypeSystem.DEFAULT;
    int defaultMaxPrecision = defaultTypeSystem.getMaxPrecision(SqlTypeName.DECIMAL);
    int defaultMaxScale = defaultTypeSystem.getMaxScale(SqlTypeName.DECIMAL);

    assertEquals(19, defaultMaxPrecision);
    assertEquals(19, defaultMaxScale);
    assertEquals(38, typeSystem.getMaxPrecision(SqlTypeName.DECIMAL));
    assertEquals(38, typeSystem.getMaxScale(SqlTypeName.DECIMAL));
  }
}
