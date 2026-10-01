package io.substrait.isthmus;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * Custom {@link RelDataTypeSystem} implementation for Substrait.
 *
 * <p>Defines type system rules such as precision, scale, and interval qualifiers for Substrait
 * integration with Calcite.
 */
public class SubstraitTypeSystem extends RelDataTypeSystemImpl {

  /** Singleton instance of Substrait type system. */
  public static final RelDataTypeSystem TYPE_SYSTEM = new SubstraitTypeSystem();

  private static final int MAX_DECIMAL_PRECISION = 38;

  /** Default type factory using the Substrait type system. */
  public static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(TYPE_SYSTEM);

  /** Interval qualifier from year to month. */
  public static final SqlIntervalQualifier YEAR_MONTH_INTERVAL =
      new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO);

  /**
   * Returns an interval qualifier from day to fractional second at the given precision.
   *
   * <p>The qualifier's precision becomes the scale of the resulting {@code INTERVAL DAY TO SECOND}
   * type, which is how a Substrait {@code interval_day<P>} carries {@code P} into Calcite.
   *
   * @param precision the fractional-second precision
   * @return the interval qualifier
   */
  public static SqlIntervalQualifier daySecondInterval(final int precision) {
    return new SqlIntervalQualifier(
        TimeUnit.DAY, -1, TimeUnit.SECOND, precision, SqlParserPos.ZERO);
  }

  /**
   * Public no-argument constructor.
   *
   * <p>Prefer the shared {@link #TYPE_SYSTEM} singleton. This constructor exists because Calcite's
   * {@link org.apache.calcite.tools.Frameworks}/Avatica machinery re-instantiates a type system
   * from its class name (via a default constructor) when it is supplied to a {@link
   * org.apache.calcite.tools.FrameworkConfig}. The type system is stateless, so additional
   * instances are equivalent to the singleton.
   */
  public SubstraitTypeSystem() {}

  /**
   * Checks that a Substrait fractional-second precision is one the Calcite type system in effect
   * allows for the type it converts to, and reports the bound it exceeds if it is not.
   *
   * @param typeSystem the type system the converted type will live under, which need not be this
   *     one
   * @param typeName the Calcite type name the Substrait type converts to
   * @param substraitTypeName the Substrait type name, for the failure message
   * @param precision the fractional-second precision carried by the Substrait type or literal
   * @throws IllegalArgumentException if the precision exceeds what the type system allows
   */
  public static void requireSupportedPrecision(
      final RelDataTypeSystem typeSystem,
      final SqlTypeName typeName,
      final String substraitTypeName,
      final int precision) {
    int maxPrecision = typeSystem.getMaxPrecision(typeName);
    if (precision > maxPrecision) {
      throw new IllegalArgumentException(
          String.format(
              "unsupported %s precision %s, max precision in Calcite type system is set to %s",
              substraitTypeName, precision, maxPrecision));
    }
  }

  /**
   * Returns the maximum precision for the given SQL type.
   *
   * <p>For the three types that carry a length across the Substrait boundary — {@link
   * SqlTypeName#CHAR}, {@link SqlTypeName#VARCHAR} and {@link SqlTypeName#BINARY}, holding {@code
   * fixedchar}, {@code varchar} and {@code fixedbinary} — this is Substrait's own limit: those
   * lengths are 32-bit integers. Calcite's default of 65536 is narrower, and the type factory caps
   * a converted type at it rather than reporting that it cannot represent the declared width.
   *
   * <p>{@link SqlTypeName#VARBINARY} is raised with them even though Substrait's {@code binary}
   * carries no length of its own, because the cap bites inside Calcite's own type unification:
   * {@link #shouldConvertRaggedUnionTypesToVarying()} is true here, so a union of fixed-width
   * binaries of different widths is unified as a {@code VARBINARY} of the widest. Left at 65536,
   * the least restrictive type of {@code BINARY(100000)} and {@code BINARY(5)} is {@code
   * VARBINARY(65536)} -- narrower than one of its own inputs, and the cap this method removes
   * reimposed.
   *
   * <p>{@link SqlTypeName#TIME}, {@link SqlTypeName#TIMESTAMP} and {@link
   * SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE} stop at 9, nanoseconds: that is the finest unit a
   * Calcite {@code TimeString} or {@code TimestampString} carries. Substrait's precisions 10 to 12
   * stay out because the type factory clamps a finer precision to the ceiling rather than reporting
   * it.
   *
   * @param typeName The {@link SqlTypeName} for which precision is requested.
   * @return Maximum precision for the type.
   */
  @Override
  public int getMaxPrecision(final SqlTypeName typeName) {
    switch (typeName) {
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
        return Integer.MAX_VALUE;
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return 9;
      case INTERVAL_DAY:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case TIME_WITH_LOCAL_TIME_ZONE:
        return 6;
      case DECIMAL:
        return 38;
    }
    return super.getMaxPrecision(typeName);
  }

  /**
   * Returns default precision for this type if supported, otherwise {@link
   * RelDataType#PRECISION_NOT_SPECIFIED} if precision is either unsupported or must be specified
   * explicitly.
   *
   * @return Default precision
   */
  @Override
  public int getDefaultPrecision(final SqlTypeName typeName) {
    switch (typeName) {
      case DECIMAL:
        return getMaxPrecision(typeName);
      default:
        return super.getDefaultPrecision(typeName);
    }
  }

  /**
   * Returns the maximum scale allowed for this type, or {@link RelDataType#SCALE_NOT_SPECIFIED} if
   * scale is not applicable for this type.
   *
   * <p>The maximum scale for the decimal type is 38.
   *
   * @return Maximum allowed scale
   */
  @Override
  public int getMaxScale(final SqlTypeName typeName) {
    switch (typeName) {
      case DECIMAL:
        return 38;
    }
    return super.getMaxScale(typeName);
  }

  /**
   * Indicates whether ragged union types should be converted to varying types.
   *
   * @return {@code true}, as Substrait requires conversion to varying types.
   */
  @Override
  public boolean shouldConvertRaggedUnionTypesToVarying() {
    return true;
  }

  /**
   * Returns the type of a {@code SUM} (and {@code $SUM0}) as the Substrait extensions declare it:
   * an integer sum is an {@code i64} and a floating-point one an {@code fp64} ({@code
   * functions_arithmetic.yaml}), where Calcite's default keeps the argument's own type, which the
   * sum overflows on real data. A decimal sum is Calcite's default, {@code DECIMAL(38, s)}, which
   * is what {@code sum:dec} declares.
   *
   * @param typeFactory the type factory
   * @param argumentType the type of the summed values
   * @return the type of the sum
   */
  @Override
  public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
    RelDataType sum;
    switch (argumentType.getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        sum = typeFactory.createSqlType(SqlTypeName.BIGINT);
        break;
      case REAL:
      case FLOAT:
      case DOUBLE:
        sum = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        break;
      default:
        return super.deriveSumType(typeFactory, argumentType);
    }
    return typeFactory.createTypeWithNullability(sum, argumentType.isNullable());
  }

  /**
   * Returns the type of an {@code AVG} as the Substrait extensions declare it: a decimal average
   * keeps its scale at precision 38, and every other type averages to itself.
   *
   * <p>Calcite routes {@code STDDEV_POP}, {@code STDDEV_SAMP}, {@code VAR_POP} and {@code VAR_SAMP}
   * through this hook as well, and it cannot tell which function called it, so over a decimal they
   * become {@code DECIMAL(38, s)} too. No extension declares a decimal {@code std_dev} or {@code
   * variance}: the argument is cast to {@code fp64} and the result cast back.
   *
   * @param typeFactory the type factory
   * @param argumentType the type of the averaged values
   * @return the type of the average
   */
  @Override
  public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
    if (argumentType.getSqlTypeName() != SqlTypeName.DECIMAL) {
      return super.deriveAvgAggType(typeFactory, argumentType);
    }
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(
            SqlTypeName.DECIMAL, MAX_DECIMAL_PRECISION, argumentType.getScale()),
        argumentType.isNullable());
  }

  /**
   * Returns the type of a decimal addition or subtraction as {@code add:dec_dec} and {@code
   * subtract:dec_dec} declare it.
   */
  @Override
  public RelDataType deriveDecimalPlusType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    return decimalResult(
        typeFactory,
        type1,
        type2,
        (p1, s1, p2, s2) -> {
          int scale = Math.max(s1, s2);
          return new int[] {scale + Math.max(p1 - s1, p2 - s2) + 1, scale};
        });
  }

  /** Returns the type of a decimal multiplication as {@code multiply:dec_dec} declares it. */
  @Override
  public RelDataType deriveDecimalMultiplyType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    return decimalResult(
        typeFactory, type1, type2, (p1, s1, p2, s2) -> new int[] {p1 + p2 + 1, s1 + s2});
  }

  /** Returns the type of a decimal division as {@code divide:dec_dec} declares it. */
  @Override
  public RelDataType deriveDecimalDivideType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    return decimalResult(
        typeFactory,
        type1,
        type2,
        (p1, s1, p2, s2) -> {
          int scale = Math.max(6, s1 + p2 + 1);
          return new int[] {p1 - s1 + p2 + scale, scale};
        });
  }

  /** Returns the type of a decimal modulus as {@code modulus:dec_dec} declares it. */
  @Override
  public RelDataType deriveDecimalModType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    return decimalResult(
        typeFactory,
        type1,
        type2,
        (p1, s1, p2, s2) -> {
          int scale = Math.max(s1, s2);
          return new int[] {Math.min(p1 - s1, p2 - s2) + scale, scale};
        });
  }

  /** The unbounded precision and scale a decimal operation's declaration starts from. */
  @FunctionalInterface
  private interface DecimalRule {
    int[] apply(int p1, int s1, int p2, int s2);
  }

  /**
   * Applies a decimal operation's rule the way the decimal extension does: a result above precision
   * 38 is capped there and gives up scale for it, down to a scale of 6 or its own scale if that is
   * smaller. Returns {@code null}, as Calcite's default does, unless both operands are exact
   * numerics and one of them is a decimal.
   */
  private static RelDataType decimalResult(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2, DecimalRule rule) {
    if (!SqlTypeUtil.isExactNumeric(type1)
        || !SqlTypeUtil.isExactNumeric(type2)
        || !(SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2))) {
      return null;
    }
    RelDataType decimal1 =
        RelDataTypeFactoryImpl.isJavaType(type1) ? typeFactory.decimalOf(type1) : type1;
    RelDataType decimal2 =
        RelDataTypeFactoryImpl.isJavaType(type2) ? typeFactory.decimalOf(type2) : type2;
    int[] initial =
        rule.apply(
            decimal1.getPrecision(),
            decimal1.getScale(),
            decimal2.getPrecision(),
            decimal2.getScale());
    int initialPrecision = initial[0];
    int initialScale = initial[1];
    int precision = Math.min(initialPrecision, MAX_DECIMAL_PRECISION);
    int scale =
        initialPrecision > MAX_DECIMAL_PRECISION
            ? Math.max(
                initialScale - (initialPrecision - MAX_DECIMAL_PRECISION),
                Math.min(initialScale, 6))
            : initialScale;
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale),
        type1.isNullable() || type2.isNullable());
  }
}
