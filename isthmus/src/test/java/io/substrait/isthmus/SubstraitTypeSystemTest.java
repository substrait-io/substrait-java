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
  void lengthCarryingTypesMaxPrecisionIsSubstraitsOwnLimit() {
    assertEquals(Integer.MAX_VALUE, typeSystem.getMaxPrecision(SqlTypeName.VARCHAR));
    assertEquals(Integer.MAX_VALUE, typeSystem.getMaxPrecision(SqlTypeName.CHAR));
    assertEquals(Integer.MAX_VALUE, typeSystem.getMaxPrecision(SqlTypeName.BINARY));
  }

  /** Substrait's {@code binary} carries no length, so VARBINARY has none to lose. */
  @Test
  void varbinaryKeepsTheCalciteDefault() {
    assertEquals(65536, typeSystem.getMaxPrecision(SqlTypeName.VARBINARY));
  }

  /**
   * Calcite's default caps a character type at 65536, which is narrower than the {@code int} length
   * Substrait declares, so a wider converted type would be silently narrowed by the type factory.
   */
  @Test
  void lengthCarryingTypesMaxPrecisionDiffersFromDefaultTypeSystem() {
    assertEquals(65536, RelDataTypeSystem.DEFAULT.getMaxPrecision(SqlTypeName.VARCHAR));
    assertEquals(65536, RelDataTypeSystem.DEFAULT.getMaxPrecision(SqlTypeName.CHAR));
    assertEquals(65536, RelDataTypeSystem.DEFAULT.getMaxPrecision(SqlTypeName.BINARY));
  }

  @Test
  void canCreateCharacterTypesWiderThanTheCalciteDefault() {
    assertEquals(100_000, TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 100_000).getPrecision());
    assertEquals(100_000, TYPE_FACTORY.createSqlType(SqlTypeName.CHAR, 100_000).getPrecision());
    assertEquals(100_000, TYPE_FACTORY.createSqlType(SqlTypeName.BINARY, 100_000).getPrecision());
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
