package io.substrait.isthmus;

import static io.substrait.isthmus.SubstraitTypeSystem.TYPE_FACTORY;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
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
    // Substrait's binary carries no length of its own, but Calcite unifies a ragged binary union
    // through this type, so a cap here caps the union.
    assertEquals(Integer.MAX_VALUE, typeSystem.getMaxPrecision(SqlTypeName.VARBINARY));
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

  /**
   * The conversion takes whatever type factory it is handed, and one built on Calcite's default
   * type system cannot hold these widths. Narrowing them is what this fix is about, so a factory
   * that would narrow is reported rather than followed.
   */
  @Test
  void aFactoryThatCannotHoldTheDeclaredLengthIsReported() {
    RelDataTypeFactory defaultFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TypeConverter.DEFAULT.toCalcite(
                    defaultFactory, TypeCreator.REQUIRED.varChar(100_000), null));
    assertTrue(e.getMessage().contains("65536"), e.getMessage());

    assertEquals(
        100_000,
        TypeConverter.DEFAULT
            .toCalcite(TYPE_FACTORY, TypeCreator.REQUIRED.varChar(100_000), null)
            .getPrecision());
  }

  /**
   * Asking the factory first cannot tell a negative width from a width it holds: with assertions
   * off it stores the negative and reports it back, and -1 is its unspecified precision besides, so
   * the answer equals what was asked for either way.
   */
  @Test
  void aNegativeLengthIsRefusedBeforeTheFactoryIsAsked() {
    assertAll(
        () -> {
          IllegalArgumentException e =
              assertThrows(
                  IllegalArgumentException.class,
                  () ->
                      TypeConverter.DEFAULT.toCalcite(
                          TYPE_FACTORY, TypeCreator.REQUIRED.varChar(-5), null));
          assertTrue(
              e.getMessage().contains("negative length, and this one is -5"), e.getMessage());
        },
        () -> {
          IllegalArgumentException e =
              assertThrows(
                  IllegalArgumentException.class,
                  () ->
                      TypeConverter.DEFAULT.toCalcite(
                          TYPE_FACTORY, TypeCreator.REQUIRED.fixedChar(-1), null));
          assertTrue(
              e.getMessage().contains("negative length, and this one is -1"), e.getMessage());
        },
        () -> {
          IllegalArgumentException e =
              assertThrows(
                  IllegalArgumentException.class,
                  () ->
                      TypeConverter.DEFAULT.toCalcite(
                          TYPE_FACTORY, TypeCreator.REQUIRED.fixedBinary(-5), null));
          assertTrue(
              e.getMessage().contains("negative length, and this one is -5"), e.getMessage());
        });
  }

  /**
   * A union of fixed-width binaries of different widths is unified as a VARBINARY, so leaving that
   * type at Calcite's default would reimpose the cap the wide types are raised past -- on a type
   * wider than one of the union's own inputs.
   */
  @Test
  void aRaggedBinaryUnionKeepsTheWidestWidth() {
    RelDataType wide = TYPE_FACTORY.createSqlType(SqlTypeName.BINARY, 100_000);
    RelDataType narrow = TYPE_FACTORY.createSqlType(SqlTypeName.BINARY, 5);

    RelDataType unified = TYPE_FACTORY.leastRestrictive(List.of(wide, narrow));

    assertEquals(SqlTypeName.VARBINARY, unified.getSqlTypeName());
    assertEquals(100_000, unified.getPrecision());
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

  /**
   * The cap reaches the conversion from SQL as well. Calcite narrows a declared width to its
   * maximum silently rather than reporting that it cannot hold it, so before this a cast wider than
   * the default came out of the conversion as a {@code varchar<65536>}.
   */
  @Test
  void aWideVarcharDeclaredInSqlKeepsItsLength() throws Exception {
    CalciteCatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE t (a VARCHAR(10))");

    Plan plan = new SqlToSubstrait().convert("SELECT CAST(a AS VARCHAR(100000)) FROM t", catalog);

    assertEquals(
        List.of(TypeCreator.NULLABLE.varChar(100000)),
        plan.getRoots().get(0).getInput().getRecordType().fields());
  }
}
