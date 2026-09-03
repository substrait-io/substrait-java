package io.substrait.isthmus;

import static io.substrait.isthmus.SubstraitTypeSystem.YEAR_MONTH_INTERVAL;

import io.substrait.function.NullableType;
import io.substrait.function.TypeExpression;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.Type.Struct;
import io.substrait.type.TypeCreator;
import io.substrait.type.TypeVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.Nullable;

/**
 * Utility for converting between Calcite {@link org.apache.calcite.rel.type.RelDataType} and
 * Substrait {@link io.substrait.type.Type}.
 *
 * <p>Supports primitive, complex, and user-defined types in both directions.
 *
 * @see UserTypeMapper
 * @see io.substrait.type.Type
 * @see org.apache.calcite.rel.type.RelDataType
 */
public class TypeConverter {

  /** The widest precision the spec gives a decimal: {@code DECIMAL<P, S>} puts P at 38 or less. */
  private static final int MAX_DECIMAL_PRECISION = 38;

  private final UserTypeMapper userTypeMapper;

  /**
   * Default {@link TypeConverter} instance that does not handle user-defined types.
   *
   * <p>Both {@link UserTypeMapper#toSubstrait(RelDataType)} and {@link
   * UserTypeMapper#toCalcite(Type.UserDefined)} return {@code null} in this default configuration.
   */
  public static final TypeConverter DEFAULT =
      new TypeConverter(
          new UserTypeMapper() {
            @Nullable
            @Override
            public Type toSubstrait(RelDataType relDataType) {
              return null;
            }

            @Nullable
            @Override
            public RelDataType toCalcite(Type.UserDefined type) {
              return null;
            }
          });

  /**
   * Creates a {@link TypeConverter} with a provided user type mapper.
   *
   * @param userTypeMapper Mapper for converting user-defined types between Calcite and Substrait.
   */
  public TypeConverter(UserTypeMapper userTypeMapper) {
    this.userTypeMapper = userTypeMapper;
  }

  /**
   * Returns the fractional-second precision of a Calcite temporal type, reading an unspecified
   * precision as zero.
   *
   * <p>A temporal type built from a Java class rather than a SQL type name -- how a reflective
   * schema exposes a {@code java.sql.Time} or {@code java.sql.Timestamp} column -- carries {@link
   * RelDataType#PRECISION_NOT_SPECIFIED}. Calcite's own {@code RexBuilder.clean} reads that
   * sentinel as precision 0; left as -1 it would travel into the Substrait type and the literals
   * built at it.
   *
   * @param type the Calcite temporal type
   * @return the fractional-second precision, never negative
   */
  public static int precisionOf(final RelDataType type) {
    int precision = type.getPrecision();
    return precision == RelDataType.PRECISION_NOT_SPECIFIED ? 0 : precision;
  }

  /**
   * Converts a Calcite {@link RelDataType} to a Substrait {@link Type}.
   *
   * @param type Calcite type to convert.
   * @return Corresponding Substrait type.
   * @throws UnsupportedOperationException if the type cannot be converted or has unsupported
   *     properties.
   */
  public Type toSubstrait(RelDataType type) {
    return toSubstrait(type, new ArrayList<>());
  }

  /**
   * Converts a Calcite {@link RelDataType} of SQL type {@link SqlTypeName#ROW} to a Substrait
   * {@link NamedStruct}.
   *
   * <p>Field names are extracted from the Calcite struct type and paired with the converted
   * Substrait struct.
   *
   * @param type Calcite struct type ({@link SqlTypeName#ROW}).
   * @return Substrait {@link NamedStruct} containing field names and struct type.
   * @throws IllegalArgumentException if {@code type} is not a struct ({@code ROW}).
   * @throws UnsupportedOperationException if any child field type cannot be converted.
   */
  public NamedStruct toNamedStruct(RelDataType type) {
    if (type.getSqlTypeName() != SqlTypeName.ROW) {
      throw new IllegalArgumentException("Expected type of struct.");
    }

    ArrayList<String> names = new ArrayList<String>();
    Struct struct = (Type.Struct) toSubstrait(type, names);
    return NamedStruct.of(names, struct);
  }

  private Type toSubstrait(RelDataType type, List<String> names) {
    // Check for user mapped types first as they may re-use SqlTypeNames
    Type userType = userTypeMapper.toSubstrait(type);
    if (userType != null) {
      return userType;
    }

    TypeCreator creator = Type.withNullability(type.isNullable());

    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return creator.BOOLEAN;
      case TINYINT:
        return creator.I8;
      case SMALLINT:
        return creator.I16;
      case INTEGER:
        return creator.I32;
      case BIGINT:
        return creator.I64;
      case REAL:
        return creator.FP32;
      case FLOAT:
      case DOUBLE:
        return creator.FP64;
      case DECIMAL:
        {
          if (type.getPrecision() > MAX_DECIMAL_PRECISION) {
            throw new UnsupportedOperationException(
                "unsupported decimal precision " + type.getPrecision());
          }
          return creator.decimal(type.getPrecision(), type.getScale());
        }
      case CHAR:
        {
          // A char or Character JavaType carries no precision of its own, which Calcite reads as
          // its default of 1. Without this a reflective schema derives fixedchar<-1>, a width
          // outside the [1..2147483647] the spec allows.
          if (type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
            return creator.fixedChar(1);
          }
          return creator.fixedChar(type.getPrecision());
        }
      case VARCHAR:
        {
          if (type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
            return creator.STRING;
          }
          return creator.varChar(type.getPrecision());
        }
      case SYMBOL:
        return creator.STRING;
      case DATE:
        return creator.DATE;
      case TIME:
        return creator.precisionTime(precisionOf(type));
      case TIMESTAMP:
        return creator.precisionTimestamp(precisionOf(type));
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return creator.precisionTimestampTZ(precisionOf(type));
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        return creator.INTERVAL_YEAR;
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        return creator.intervalDay(type.getScale());
      case VARBINARY:
        return creator.BINARY;
      case BINARY:
        return creator.fixedBinary(type.getPrecision());
      case MAP:
        {
          MapSqlType map = (MapSqlType) type;
          return creator.map(
              toSubstrait(map.getKeyType(), names), toSubstrait(map.getValueType(), names));
        }
      case ROW:
        {
          ArrayList<Type> children = new ArrayList<Type>();
          for (RelDataTypeField field : type.getFieldList()) {
            names.add(field.getName());
            children.add(toSubstrait(field.getType(), names));
          }
          return creator.struct(children);
        }
      case ARRAY:
        return creator.list(toSubstrait(type.getComponentType(), names));
      default:
        throw new UnsupportedOperationException(
            String.format("Unable to convert the type " + type.toString()));
    }
  }

  /**
   * Converts a Substrait {@link TypeExpression} to a Calcite {@link RelDataType}.
   *
   * @param relDataTypeFactory Calcite type factory.
   * @param typeExpression Substrait type expression to convert.
   * @return Calcite relational type.
   * @throws UnsupportedOperationException if the expression contains unsupported precision or
   *     user-defined types cannot be mapped.
   * @throws IllegalArgumentException if a declared length or precision is negative, or the given
   *     factory cannot hold it.
   */
  public RelDataType toCalcite(
      RelDataTypeFactory relDataTypeFactory, TypeExpression typeExpression) {
    return toCalcite(relDataTypeFactory, typeExpression, null);
  }

  /**
   * Converts a Substrait {@link TypeExpression} to a Calcite {@link RelDataType}, with optional
   * field names for DFS/nested structs.
   *
   * @param relDataTypeFactory Calcite type factory.
   * @param typeExpression Substrait type expression to convert.
   * @param dfsFieldNames Optional list of field names to apply to struct fields, in DFS order.
   * @return Calcite relational type.
   * @throws UnsupportedOperationException if the expression contains unsupported precision or
   *     user-defined types cannot be mapped.
   * @throws IllegalArgumentException if a declared length or precision is negative, or the given
   *     factory cannot hold it.
   */
  public RelDataType toCalcite(
      RelDataTypeFactory relDataTypeFactory,
      TypeExpression typeExpression,
      List<String> dfsFieldNames) {
    return typeExpression.accept(
        new ToRelDataType(relDataTypeFactory, userTypeMapper, dfsFieldNames, 0));
  }

  private static class ToRelDataType
      extends TypeVisitor.TypeThrowsVisitor<RelDataType, RuntimeException> {

    private final RelDataTypeFactory typeFactory;
    private final UserTypeMapper userTypeMapper;
    private final List<String> fieldNames;
    private int fieldNamePosition;
    private boolean withinStruct;

    public ToRelDataType(
        final RelDataTypeFactory type,
        final UserTypeMapper userTypeMapper,
        final List<String> fieldNames,
        int fieldNamePosition) {
      super("Unknown expression type.");
      this.typeFactory = type;
      this.userTypeMapper = userTypeMapper;
      this.fieldNames = fieldNames;
      this.fieldNamePosition = fieldNamePosition;
    }

    @Override
    public RelDataType visit(Type.Bool expr) {
      return t(n(expr), SqlTypeName.BOOLEAN);
    }

    @Override
    public RelDataType visit(Type.I8 expr) {
      return t(n(expr), SqlTypeName.TINYINT);
    }

    @Override
    public RelDataType visit(Type.I16 expr) {
      return t(n(expr), SqlTypeName.SMALLINT);
    }

    @Override
    public RelDataType visit(Type.I32 expr) {
      return t(n(expr), SqlTypeName.INTEGER);
    }

    @Override
    public RelDataType visit(Type.I64 expr) {
      return t(n(expr), SqlTypeName.BIGINT);
    }

    @Override
    public RelDataType visit(Type.FP32 expr) {
      return t(n(expr), SqlTypeName.REAL);
    }

    @Override
    public RelDataType visit(Type.FP64 expr) {
      return t(n(expr), SqlTypeName.DOUBLE);
    }

    @Override
    public RelDataType visit(Type.Str expr) {
      return t(n(expr), SqlTypeName.VARCHAR);
    }

    @Override
    public RelDataType visit(Type.Binary expr) {
      return t(n(expr), SqlTypeName.VARBINARY);
    }

    @Override
    public RelDataType visit(Type.Date expr) {
      return t(n(expr), SqlTypeName.DATE);
    }

    @Override
    public RelDataType visit(Type.PrecisionTime expr) {
      SubstraitTypeSystem.requireSupportedPrecision(
          typeFactory.getTypeSystem(), SqlTypeName.TIME, "precision_time", expr.precision());
      return t(n(expr), SqlTypeName.TIME, expr.precision());
    }

    @Override
    public RelDataType visit(Type.PrecisionTimestamp expr) {
      SubstraitTypeSystem.requireSupportedPrecision(
          typeFactory.getTypeSystem(),
          SqlTypeName.TIMESTAMP,
          "precision_timestamp",
          expr.precision());
      return t(n(expr), SqlTypeName.TIMESTAMP, expr.precision());
    }

    @Override
    public RelDataType visit(Type.PrecisionTimestampTZ expr) throws RuntimeException {
      SubstraitTypeSystem.requireSupportedPrecision(
          typeFactory.getTypeSystem(),
          SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
          "precision_timestamp_tz",
          expr.precision());
      return t(n(expr), SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, expr.precision());
    }

    @Override
    public RelDataType visit(Type.IntervalYear expr) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlIntervalType(YEAR_MONTH_INTERVAL), n(expr));
    }

    @Override
    public RelDataType visit(Type.IntervalDay expr) {
      // getMaxScale, not getMaxPrecision: for an interval the latter bounds the leading field,
      // while the fractional-second bound is the scale of the INTERVAL_DAY_SECOND type produced
      // here. That is also the knob SqlValidatorImpl checks an interval qualifier against, so this
      // accepts exactly what SQL written as DAY TO SECOND(P) parses to.
      int maxPrecision = typeFactory.getTypeSystem().getMaxScale(SqlTypeName.INTERVAL_DAY_SECOND);
      if (expr.precision() < 0 || expr.precision() > maxPrecision) {
        throw new IllegalArgumentException(
            String.format(
                "unsupported interval_day precision %s, Calcite type system allows 0 to %s",
                expr.precision(), maxPrecision));
      }
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlIntervalType(
              SubstraitTypeSystem.daySecondInterval(expr.precision())),
          n(expr));
    }

    @Override
    public RelDataType visit(Type.FixedChar expr) {
      return withLength(n(expr), SqlTypeName.CHAR, expr.length());
    }

    @Override
    public RelDataType visit(Type.VarChar expr) {
      return withLength(n(expr), SqlTypeName.VARCHAR, expr.length());
    }

    @Override
    public RelDataType visit(Type.FixedBinary expr) {
      return withLength(n(expr), SqlTypeName.BINARY, expr.length());
    }

    /**
     * Returns the type the given factory builds for a declared length, having checked that it holds
     * it. A factory caps a width at its type system's maximum without saying so, and this
     * conversion takes whatever factory it is handed, so a factory whose limits are not Substrait's
     * would otherwise return a type narrower than the plan declares.
     *
     * <p>A negative length is refused before the factory is asked, because asking tells us nothing:
     * with assertions off the factory stores the negative and reports it back, so the width below
     * certifies itself, and with them on Calcite raises a bare {@code AssertionError} in place of
     * this message. A length of -1 is Calcite's unspecified precision besides, so the factory
     * answers with an unparameterised type whose precision equals what was asked for. A zero length
     * is left to the factory: the spec puts a fixedchar's width at 1 or more, but Calcite types the
     * empty character literal as a {@code CHAR(0)}, so plans carrying one exist.
     *
     * @param nullable whether the type is nullable
     * @param typeName the Calcite type name to build
     * @param length the declared length
     * @return the built type
     * @throws IllegalArgumentException if the length is negative, or the factory built a type of
     *     another length
     */
    private RelDataType withLength(boolean nullable, SqlTypeName typeName, int length) {
      if (length < 0) {
        throw new IllegalArgumentException(
            String.format(
                "A %s cannot declare a negative length, and this one is %d", typeName, length));
      }
      RelDataType type = t(nullable, typeName, length);
      if (type.getPrecision() != length) {
        throw new IllegalArgumentException(
            String.format(
                "The type factory cannot hold %s(%d), which it narrowed to %s; its type system"
                    + " allows up to %d",
                typeName, length, type, typeFactory.getTypeSystem().getMaxPrecision(typeName)));
      }
      return type;
    }

    @Override
    public RelDataType visit(Type.Decimal expr) {
      // Before the factory, for the reason the lengths are: -1 is Calcite's unspecified precision,
      // so a negative one is answered with an unparameterised DECIMAL whose precision reads back as
      // the type system's maximum. A zero or negative scale Calcite reports itself.
      if (expr.precision() < 0) {
        throw new IllegalArgumentException(
            String.format(
                "A decimal cannot declare a negative precision, and this one is %d",
                expr.precision()));
      }
      // The spec's own ceiling, not just the factory's: handed a type system whose DECIMAL maximum
      // is above it, the factory builds the type and the outbound conversion above then refuses it,
      // so the type would convert in and have no way back.
      if (expr.precision() > MAX_DECIMAL_PRECISION) {
        throw new IllegalArgumentException(
            String.format(
                "A decimal cannot declare a precision of %d, above the %d the spec allows",
                expr.precision(), MAX_DECIMAL_PRECISION));
      }
      SubstraitTypeSystem.requireSupportedPrecision(
          typeFactory.getTypeSystem(), SqlTypeName.DECIMAL, "decimal", expr.precision());
      // The spec puts a decimal's scale in [0..P]. No factory reports a scale above the precision:
      // Calcite builds the type as asked, and its own maximum cannot catch it either, since both
      // type systems here set maxScale equal to maxPrecision.
      if (expr.scale() > expr.precision()) {
        throw new IllegalArgumentException(
            String.format(
                "A decimal cannot declare a scale of %d above its precision of %d",
                expr.scale(), expr.precision()));
      }
      return t(n(expr), SqlTypeName.DECIMAL, expr.precision(), expr.scale());
    }

    @Override
    public RelDataType visit(Type.Struct expr) {
      if (withinStruct) {
        throw new IllegalStateException("Visitor can't be re-used for nested structs.");
      }
      withinStruct = true;
      try {
        List<RelDataType> fieldTypes = new ArrayList<>();
        List<String> localFieldNames = new ArrayList<>();
        for (TypeExpression field : expr.fields()) {
          localFieldNames.add(
              fieldNames == null ? "f" + fieldNamePosition : fieldNames.get(fieldNamePosition));
          fieldNamePosition++;
          ToRelDataType childVisitor =
              new ToRelDataType(typeFactory, userTypeMapper, fieldNames, fieldNamePosition);
          fieldTypes.add(field.accept(childVisitor));
          fieldNamePosition = childVisitor.fieldNamePosition;
        }

        return n(expr, typeFactory.createStructType(fieldTypes, localFieldNames));

      } finally {
        withinStruct = false;
      }
    }

    @Override
    public RelDataType visit(Type.ListType expr) {
      return n(expr, typeFactory.createArrayType(expr.elementType().accept(this), -1));
    }

    @Override
    public RelDataType visit(Type.Map expr) {
      return n(expr, typeFactory.createMapType(expr.key().accept(this), expr.value().accept(this)));
    }

    @Override
    public RelDataType visit(Type.UserDefined expr) throws RuntimeException {
      RelDataType type = userTypeMapper.toCalcite(expr);
      if (type != null) {
        return type;
      }
      throw new UnsupportedOperationException(
          String.format("Unable to map user-defined type: %s", expr));
    }

    private boolean n(NullableType type) {
      return type.nullable();
    }

    private RelDataType t(boolean nullable, SqlTypeName typeName, Integer... props) {
      final RelDataType baseType;
      if (props.length == 0) {
        baseType = typeFactory.createSqlType(typeName);
      } else if (props.length == 1) {
        baseType = typeFactory.createSqlType(typeName, props[0]);
      } else if (props.length == 2) {
        baseType = typeFactory.createSqlType(typeName, props[0], props[1]);
      } else {
        throw new IllegalArgumentException(
            "Unexpected properties length: " + Arrays.toString(props));
      }

      return typeFactory.createTypeWithNullability(baseType, nullable);
    }

    private RelDataType n(Type substraitType, RelDataType type) {
      return typeFactory.createTypeWithNullability(type, n(substraitType));
    }
  }
}
