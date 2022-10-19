package io.substrait.type.proto;

import io.substrait.function.TypeExpressionVisitor;
import io.substrait.proto.Type;

public class TypeProtoConverter extends BaseProtoConverter<Type, Integer> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TypeProtoConverter.class);

  public static TypeExpressionVisitor<Type, RuntimeException> INSTANCE = new TypeProtoConverter();

  public TypeProtoConverter() {
    super("Type literals cannot contain parameters or expressions.");
  }

  private static final BaseProtoTypes<Type, Integer> NULLABLE =
      new Types(Type.Nullability.NULLABILITY_NULLABLE);
  private static final BaseProtoTypes<Type, Integer> REQUIRED =
      new Types(Type.Nullability.NULLABILITY_REQUIRED);

  @Override
  public BaseProtoTypes<Type, Integer> typeContainer(final boolean nullable) {
    return nullable ? NULLABLE : REQUIRED;
  }

  private static class Types extends BaseProtoTypes<Type, Integer> {

    public Types(final Type.Nullability nullability) {
      super(nullability);
    }

    public Type fixedChar(Integer len) {
      return wrap(Type.FixedChar.newBuilder().setLength(len).setNullability(nullability).build());
    }

    @Override
    public Type typeParam(final String name) {
      throw new UnsupportedOperationException(
          "It is not possible to use parameters in basic types.");
    }

    @Override
    public Integer integerParam(final String name) {
      throw new UnsupportedOperationException(
          "It is not possible to use parameters in basic types.");
    }

    public Type varChar(Integer len) {
      return wrap(Type.VarChar.newBuilder().setLength(len).setNullability(nullability).build());
    }

    public Type fixedBinary(Integer len) {
      return wrap(Type.FixedBinary.newBuilder().setLength(len).setNullability(nullability).build());
    }

    public Type decimal(Integer scale, Integer precision) {
      return wrap(
          Type.Decimal.newBuilder()
              .setScale(scale)
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public Type struct(Iterable<Type> types) {
      return wrap(Type.Struct.newBuilder().addAllTypes(types).setNullability(nullability).build());
    }

    public Type list(Type type) {
      return wrap(Type.List.newBuilder().setType(type).setNullability(nullability).build());
    }

    public Type map(Type key, Type value) {
      return wrap(
          Type.Map.newBuilder().setKey(key).setValue(value).setNullability(nullability).build());
    }

    @Override
    protected Type wrap(final Object o) {
      Type.Builder bldr = Type.newBuilder();
      if (o instanceof Type.Boolean) {
        return bldr.setBool((Type.Boolean) o).build();
      } else if (o instanceof Type.I8) {
        return bldr.setI8((Type.I8) o).build();
      } else if (o instanceof Type.I16) {
        return bldr.setI16((Type.I16) o).build();
      } else if (o instanceof Type.I32) {
        return bldr.setI32((Type.I32) o).build();
      } else if (o instanceof Type.I64) {
        return bldr.setI64((Type.I64) o).build();
      } else if (o instanceof Type.FP32) {
        return bldr.setFp32((Type.FP32) o).build();
      } else if (o instanceof Type.FP64) {
        return bldr.setFp64((Type.FP64) o).build();
      } else if (o instanceof Type.String) {
        return bldr.setString((Type.String) o).build();
      } else if (o instanceof Type.Binary) {
        return bldr.setBinary((Type.Binary) o).build();
      } else if (o instanceof Type.Timestamp) {
        return bldr.setTimestamp((Type.Timestamp) o).build();
      } else if (o instanceof Type.Date) {
        return bldr.setDate((Type.Date) o).build();
      } else if (o instanceof Type.Time) {
        return bldr.setTime((Type.Time) o).build();
      } else if (o instanceof Type.TimestampTZ) {
        return bldr.setTimestampTz((Type.TimestampTZ) o).build();
      } else if (o instanceof Type.IntervalYear) {
        return bldr.setIntervalYear((Type.IntervalYear) o).build();
      } else if (o instanceof Type.IntervalDay) {
        return bldr.setIntervalDay((Type.IntervalDay) o).build();
      } else if (o instanceof Type.FixedChar) {
        return bldr.setFixedChar((Type.FixedChar) o).build();
      } else if (o instanceof Type.VarChar) {
        return bldr.setVarchar((Type.VarChar) o).build();
      } else if (o instanceof Type.FixedBinary) {
        return bldr.setFixedBinary((Type.FixedBinary) o).build();
      } else if (o instanceof Type.Decimal) {
        return bldr.setDecimal((Type.Decimal) o).build();
      } else if (o instanceof Type.Struct) {
        return bldr.setStruct((Type.Struct) o).build();
      } else if (o instanceof Type.List) {
        return bldr.setList((Type.List) o).build();
      } else if (o instanceof Type.Map) {
        return bldr.setMap((Type.Map) o).build();
      } else if (o instanceof Type.UUID) {
        return bldr.setUuid((Type.UUID) o).build();
      }
      throw new UnsupportedOperationException("Unable to wrap type of " + o.getClass());
    }

    @Override
    protected Integer i(final int integerValue) {
      return integerValue;
    }
  }
}
