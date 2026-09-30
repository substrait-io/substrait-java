package io.substrait.isthmus;

import static io.substrait.isthmus.SubstraitTypeSystem.TYPE_FACTORY;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ToTypeString;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class SubstraitTypeSystemTest {

  private static final TypeCreator R = TypeCreator.REQUIRED;

  private final RelDataTypeSystem typeSystem = SubstraitTypeSystem.TYPE_SYSTEM;

  /**
   * Decimal addition, subtraction, multiplication, division and modulus derive what the decimal
   * extension declares, checked against the catalog's own derivation over precisions and scales on
   * both sides of the 38 cap, where the result gives up scale.
   */
  @Test
  void decimalArithmeticDerivesWhatTheExtensionDeclares() {
    List<Integer> precisions = List.of(1, 5, 7, 12, 19, 28, 37, 38);
    for (int p1 : precisions) {
      for (int s1 : scales(p1)) {
        for (int p2 : precisions) {
          for (int s2 : scales(p2)) {
            RelDataType left = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, p1, s1);
            RelDataType right = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, p2, s2);
            List<Type> operands = List.of(R.decimal(p1, s1), R.decimal(p2, s2));
            assertEquals(
                declared("add:dec_dec", operands),
                substrait(typeSystem.deriveDecimalPlusType(TYPE_FACTORY, left, right)));
            assertEquals(
                declared("subtract:dec_dec", operands),
                substrait(typeSystem.deriveDecimalPlusType(TYPE_FACTORY, left, right)));
            assertEquals(
                declared("multiply:dec_dec", operands),
                substrait(typeSystem.deriveDecimalMultiplyType(TYPE_FACTORY, left, right)));
            assertEquals(
                declared("divide:dec_dec", operands),
                substrait(typeSystem.deriveDecimalDivideType(TYPE_FACTORY, left, right)));
            assertEquals(
                declared("modulus:dec_dec", operands),
                substrait(typeSystem.deriveDecimalModType(TYPE_FACTORY, left, right)));
          }
        }
      }
    }
  }

  /**
   * An integer operand counts as the decimal that holds its type, the one isthmus casts it to:
   * {@code decimal(10,0)} for an INTEGER and {@code decimal(19,0)} for a BIGINT, whatever the type
   * system's own decimal precision.
   */
  @Test
  void decimalArithmeticWithAnIntegerOperandDerivesWhatTheExtensionDeclares() {
    RelDataType decimal = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 7, 2);
    for (SqlTypeName integer : List.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
      RelDataType other = TYPE_FACTORY.createSqlType(integer);
      List<Type> operands =
          List.of(R.decimal(7, 2), R.decimal(integer == SqlTypeName.INTEGER ? 10 : 19, 0));
      assertEquals(
          declared("add:dec_dec", operands),
          substrait(typeSystem.deriveDecimalPlusType(TYPE_FACTORY, decimal, other)));
      assertEquals(
          declared("multiply:dec_dec", operands),
          substrait(typeSystem.deriveDecimalMultiplyType(TYPE_FACTORY, decimal, other)));
      assertEquals(
          declared("divide:dec_dec", operands),
          substrait(typeSystem.deriveDecimalDivideType(TYPE_FACTORY, decimal, other)));
    }
  }

  /**
   * SUM and AVG derive what the arithmetic extensions declare. The aggregate function sets the
   * result's nullability, so only the type is compared.
   */
  @Test
  void sumAndAvgDeriveWhatTheExtensionsDeclare() {
    List<SqlTypeName> numerics =
        List.of(
            SqlTypeName.TINYINT,
            SqlTypeName.SMALLINT,
            SqlTypeName.INTEGER,
            SqlTypeName.BIGINT,
            SqlTypeName.REAL,
            SqlTypeName.DOUBLE);
    for (SqlTypeName name : numerics) {
      RelDataType argument = TYPE_FACTORY.createSqlType(name);
      assertAggregate("sum", argument, typeSystem.deriveSumType(TYPE_FACTORY, argument));
      assertAggregate("avg", argument, typeSystem.deriveAvgAggType(TYPE_FACTORY, argument));
    }
    for (int precision : List.of(1, 7, 38)) {
      for (int scale : scales(precision)) {
        RelDataType argument = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        assertAggregate("sum", argument, typeSystem.deriveSumType(TYPE_FACTORY, argument));
        assertAggregate("avg", argument, typeSystem.deriveAvgAggType(TYPE_FACTORY, argument));
      }
    }
  }

  /**
   * The same in a query: isthmus's own SUM and AVG take their types from the type system, where
   * they used to repeat the argument's. The average stays nullable, the right answer for an empty
   * group, although avg:dec declares a required result.
   */
  @Test
  void sumAndAvgInAQueryTakeTheTypeSystemsTypes() throws Exception {
    CalciteCatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE t (d DECIMAL(7, 2), i INT)");
    Plan plan = new SqlToSubstrait().convert("SELECT SUM(d), AVG(d), SUM(i) FROM t", catalog);

    assertEquals(
        List.of(
            TypeCreator.NULLABLE.decimal(38, 2),
            TypeCreator.NULLABLE.decimal(38, 2),
            TypeCreator.NULLABLE.I64),
        plan.getRoots().get(0).getInput().getRecordType().fields());
  }

  private static List<Integer> scales(int precision) {
    return List.of(0, precision / 2, precision).stream().distinct().toList();
  }

  private static Type declared(String key, List<Type> operands) {
    return DefaultExtensionCatalog.DEFAULT_COLLECTION
        .getScalarFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL, key))
        .resolveType(operands);
  }

  private static Type substrait(RelDataType type) {
    return TypeConverter.DEFAULT.toSubstrait(type);
  }

  private static void assertAggregate(String name, RelDataType argument, RelDataType derived) {
    Type operand = substrait(argument);
    String urn =
        argument.getSqlTypeName() == SqlTypeName.DECIMAL
            ? DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL
            : DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC;
    String key = name + ":" + operand.accept(ToTypeString.INSTANCE);
    Type declared =
        DefaultExtensionCatalog.DEFAULT_COLLECTION
            .getAggregateFunction(SimpleExtension.FunctionAnchor.of(urn, key))
            .resolveType(List.of(operand));
    assertTrue(
        declared.equalsIgnoringNullability(substrait(derived)),
        () -> key + " declares " + declared + " but derives " + derived.getFullTypeString());
  }

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
    // Nanoseconds: the finest unit a Calcite TimestampString carries, and what the type factory
    // builds a TIMESTAMP at once the ceiling allows it. Picoseconds clamp rather than throw, so
    // Substrait's 10 to 12 stay out.
    assertEquals(9, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP));
    assertEquals(9, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
  }

  @Test
  void timeMaxPrecision() {
    assertEquals(9, typeSystem.getMaxPrecision(SqlTypeName.TIME));
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
    assertTrue(e.getMessage().contains("allows up to 65536"), e.getMessage());

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
   * A type system whose DECIMAL maximum is above the spec's 38 would let a wider decimal through
   * the factory, and {@code toSubstrait} refuses it on the way back -- so the type would convert in
   * with no way out. The bound is the spec's rather than the factory's for that reason.
   */
  @Test
  void aDecimalPrecisionAboveTheSpecsCeilingIsRefused() {
    RelDataTypeFactory wideFactory =
        new SqlTypeFactoryImpl(
            new RelDataTypeSystemImpl() {
              @Override
              public int getMaxPrecision(SqlTypeName typeName) {
                return typeName == SqlTypeName.DECIMAL ? 76 : super.getMaxPrecision(typeName);
              }

              @Override
              public int getMaxScale(SqlTypeName typeName) {
                return typeName == SqlTypeName.DECIMAL ? 76 : super.getMaxScale(typeName);
              }
            });

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TypeConverter.DEFAULT.toCalcite(
                    wideFactory, TypeCreator.REQUIRED.decimal(45, 2), null));

    assertTrue(e.getMessage().contains("above the 38 the spec allows"), e.getMessage());
  }

  /**
   * Two ends the factory does not report. A precision of -1 is Calcite's unspecified precision, so
   * it answers with an unparameterised DECIMAL whose precision reads back as the type system's
   * maximum. A scale above the precision is outside the spec's {@code 0 <= S <= P} and Calcite
   * builds it anyway, where its own maximum would not catch it: both type systems here set {@code
   * maxScale} equal to {@code maxPrecision}, so a scale within the precision is within that too.
   * Calcite reports a zero or negative scale and a zero precision itself.
   */
  @Test
  void aDecimalParameterOutsideItsDeclaredBoundsIsRefused() {
    assertAll(
        () -> {
          IllegalArgumentException e =
              assertThrows(
                  IllegalArgumentException.class,
                  () ->
                      TypeConverter.DEFAULT.toCalcite(
                          TYPE_FACTORY, TypeCreator.REQUIRED.decimal(-1, 0), null));
          assertTrue(
              e.getMessage().contains("negative precision, and this one is -1"), e.getMessage());
        },
        () -> {
          IllegalArgumentException e =
              assertThrows(
                  IllegalArgumentException.class,
                  () ->
                      TypeConverter.DEFAULT.toCalcite(
                          TYPE_FACTORY, TypeCreator.REQUIRED.decimal(19, 25), null));
          assertTrue(e.getMessage().contains("scale of 25 above its precision"), e.getMessage());
        },
        () ->
            assertEquals(
                19,
                TypeConverter.DEFAULT
                    .toCalcite(TYPE_FACTORY, TypeCreator.REQUIRED.decimal(19, 19), null)
                    .getScale()));
  }

  /**
   * The raised maximum is also Calcite's overflow threshold for concatenation, in {@code
   * ReturnTypes.DYADIC_STRING_SUM_PRECISION}: a sum of widths past it falls back to an
   * unparameterised type. So two columns nowhere near the old cap decide the result type between
   * them, and the conversion emitted a {@code string} for them before.
   */
  @Test
  void concatenatingTwoVarcharsKeepsTheSumOfTheirWidths() throws Exception {
    CalciteCatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE t (a VARCHAR(40000), b VARCHAR(40000))");

    Plan plan = new SqlToSubstrait().convert("SELECT a || b FROM t", catalog);

    assertEquals(
        List.of(TypeCreator.NULLABLE.varChar(80000)),
        plan.getRoots().get(0).getInput().getRecordType().fields());
  }

  /**
   * A decimal loses its precision to a foreign factory the same silent way a length does: Calcite's
   * default type system caps it at 19, and the narrowed type reads back as one the plan never
   * declared.
   */
  @Test
  void aFactoryThatCannotHoldTheDeclaredPrecisionIsReported() {
    RelDataTypeFactory defaultFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TypeConverter.DEFAULT.toCalcite(
                    defaultFactory, TypeCreator.REQUIRED.decimal(38, 10), null));
    assertTrue(e.getMessage().contains("is set to 19"), e.getMessage());

    assertEquals(
        38,
        TypeConverter.DEFAULT
            .toCalcite(TYPE_FACTORY, TypeCreator.REQUIRED.decimal(38, 10), null)
            .getPrecision());
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
