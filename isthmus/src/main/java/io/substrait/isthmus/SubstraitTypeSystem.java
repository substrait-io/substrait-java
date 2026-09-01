package io.substrait.isthmus;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Custom {@link RelDataTypeSystem} implementation for Substrait.
 *
 * <p>Defines type system rules such as precision, scale, and interval qualifiers for Substrait
 * integration with Calcite.
 */
public class SubstraitTypeSystem extends RelDataTypeSystemImpl {

  /** Singleton instance of Substrait type system. */
  public static final RelDataTypeSystem TYPE_SYSTEM = new SubstraitTypeSystem();

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
      case INTERVAL_DAY:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
}
