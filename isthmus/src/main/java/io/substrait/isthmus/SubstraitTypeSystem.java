package io.substrait.isthmus;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
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

  /** Interval qualifier from day to fractional second at microsecond precision. */
  public static final SqlIntervalQualifier DAY_SECOND_INTERVAL =
      new SqlIntervalQualifier(TimeUnit.DAY, -1, TimeUnit.SECOND, 6, SqlParserPos.ZERO);

  /** Private constructor to enforce singleton usage. */
  private SubstraitTypeSystem() {}

  /**
   * Returns the maximum precision for the given SQL type.
   *
   * @param typeName The {@link SqlTypeName} for which precision is requested.
   * @return Maximum precision for the type.
   */
  @Override
  public int getMaxPrecision(final SqlTypeName typeName) {
    switch (typeName) {
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
   * Returns the maximum numeric scale supported by this type system.
   *
   * @return Maximum numeric scale (38).
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
   * Returns the maximum numeric precision supported by this type system.
   *
   * @return Maximum numeric precision (38).
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
