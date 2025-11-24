package io.substrait.type.proto;

import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.Type;

/** Convert from {@link io.substrait.type.Type} to {@link io.substrait.proto.Type} */
public class TypeProtoConverter extends BaseProtoConverter<Type, Integer> {
  // Instance fields (not static) because Types is a non-static inner class that calls
  // TypeProtoConverter.this.toProto() to recursively convert nested type parameters.
  // Each converter instance needs its own Types instances to ensure type registrations
  // use the correct ExtensionCollector.
  private final BaseProtoTypes<Type, Integer> NULLABLE;
  private final BaseProtoTypes<Type, Integer> REQUIRED;

  public TypeProtoConverter(ExtensionCollector extensionCollector) {
    super(extensionCollector, "Type literals cannot contain parameters or expressions.");
    NULLABLE = new Types(Type.Nullability.NULLABILITY_NULLABLE);
    REQUIRED = new Types(Type.Nullability.NULLABILITY_REQUIRED);
  }

  public io.substrait.proto.Type toProto(io.substrait.type.Type type) {
    return type.accept(this);
  }

  public io.substrait.proto.Type.Parameter toProto(io.substrait.type.Type.Parameter parameter) {
    if (parameter instanceof io.substrait.type.Type.ParameterNull) {
      return Type.Parameter.newBuilder()
          .setNull(com.google.protobuf.Empty.getDefaultInstance())
          .build();
    } else if (parameter instanceof io.substrait.type.Type.ParameterDataType) {
      io.substrait.type.Type.ParameterDataType dataType =
          (io.substrait.type.Type.ParameterDataType) parameter;
      return Type.Parameter.newBuilder().setDataType(toProto(dataType.type())).build();
    } else if (parameter instanceof io.substrait.type.Type.ParameterBooleanValue) {
      io.substrait.type.Type.ParameterBooleanValue boolValue =
          (io.substrait.type.Type.ParameterBooleanValue) parameter;
      return Type.Parameter.newBuilder().setBoolean(boolValue.value()).build();
    } else if (parameter instanceof io.substrait.type.Type.ParameterIntegerValue) {
      io.substrait.type.Type.ParameterIntegerValue intValue =
          (io.substrait.type.Type.ParameterIntegerValue) parameter;
      return Type.Parameter.newBuilder().setInteger(intValue.value()).build();
    } else if (parameter instanceof io.substrait.type.Type.ParameterEnumValue) {
      io.substrait.type.Type.ParameterEnumValue enumValue =
          (io.substrait.type.Type.ParameterEnumValue) parameter;
      return Type.Parameter.newBuilder().setEnum(enumValue.value()).build();
    } else if (parameter instanceof io.substrait.type.Type.ParameterStringValue) {
      io.substrait.type.Type.ParameterStringValue stringValue =
          (io.substrait.type.Type.ParameterStringValue) parameter;
      return Type.Parameter.newBuilder().setString(stringValue.value()).build();
    } else {
      throw new UnsupportedOperationException(
          "Unsupported parameter type: " + parameter.getClass());
    }
  }

  @Override
  public BaseProtoTypes<Type, Integer> typeContainer(final boolean nullable) {
    return nullable ? NULLABLE : REQUIRED;
  }

  /**
   * Non-static inner class that can access the outer TypeProtoConverter instance.
   *
   * <p>This class must be non-static to access TypeProtoConverter.this.toProto() for converting
   * nested type parameters (e.g., ParameterDataType containing another Type). Being non-static
   * means instances are bound to a specific outer TypeProtoConverter instance, ensuring parameter
   * conversions use the correct ExtensionCollector.
   */
  private class Types extends BaseProtoTypes<Type, Integer> {

    public Types(final Type.Nullability nullability) {
      super(nullability);
    }

    @Override
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

    @Override
    public Type varChar(Integer len) {
      return wrap(Type.VarChar.newBuilder().setLength(len).setNullability(nullability).build());
    }

    @Override
    public Type fixedBinary(Integer len) {
      return wrap(Type.FixedBinary.newBuilder().setLength(len).setNullability(nullability).build());
    }

    @Override
    public Type decimal(Integer scale, Integer precision) {
      return wrap(
          Type.Decimal.newBuilder()
              .setScale(scale)
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public Type intervalDay(Integer precision) {
      return wrap(
          Type.IntervalDay.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public Type intervalCompound(Integer precision) {
      return wrap(
          Type.IntervalCompound.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public Type precisionTime(Integer precision) {
      return wrap(
          Type.PrecisionTime.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public Type precisionTimestamp(Integer precision) {
      return wrap(
          Type.PrecisionTimestamp.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public Type precisionTimestampTZ(Integer precision) {
      return wrap(
          Type.PrecisionTimestampTZ.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public Type struct(Iterable<Type> types) {
      return wrap(Type.Struct.newBuilder().addAllTypes(types).setNullability(nullability).build());
    }

    @Override
    public Type list(Type type) {
      return wrap(Type.List.newBuilder().setType(type).setNullability(nullability).build());
    }

    @Override
    public Type map(Type key, Type value) {
      return wrap(
          Type.Map.newBuilder().setKey(key).setValue(value).setNullability(nullability).build());
    }

    @Override
    public Type userDefined(int ref) {
      return wrap(
          Type.UserDefined.newBuilder().setTypeReference(ref).setNullability(nullability).build());
    }

    @Override
    public Type userDefined(
        int ref, java.util.List<io.substrait.type.Type.Parameter> typeParameters) {
      return wrap(
          Type.UserDefined.newBuilder()
              .setTypeReference(ref)
              .setNullability(nullability)
              .addAllTypeParameters(
                  typeParameters.stream()
                      .map(TypeProtoConverter.this::toProto)
                      .collect(java.util.stream.Collectors.toList()))
              .build());
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
      } else if (o instanceof Type.IntervalCompound) {
        return bldr.setIntervalCompound((Type.IntervalCompound) o).build();
      } else if (o instanceof Type.FixedChar) {
        return bldr.setFixedChar((Type.FixedChar) o).build();
      } else if (o instanceof Type.VarChar) {
        return bldr.setVarchar((Type.VarChar) o).build();
      } else if (o instanceof Type.FixedBinary) {
        return bldr.setFixedBinary((Type.FixedBinary) o).build();
      } else if (o instanceof Type.Decimal) {
        return bldr.setDecimal((Type.Decimal) o).build();
      } else if (o instanceof Type.PrecisionTimestamp) {
        return bldr.setPrecisionTimestamp((Type.PrecisionTimestamp) o).build();
      } else if (o instanceof Type.PrecisionTimestampTZ) {
        return bldr.setPrecisionTimestampTz((Type.PrecisionTimestampTZ) o).build();
      } else if (o instanceof Type.Struct) {
        return bldr.setStruct((Type.Struct) o).build();
      } else if (o instanceof Type.List) {
        return bldr.setList((Type.List) o).build();
      } else if (o instanceof Type.Map) {
        return bldr.setMap((Type.Map) o).build();
      } else if (o instanceof Type.UUID) {
        return bldr.setUuid((Type.UUID) o).build();
      } else if (o instanceof Type.UserDefined) {
        return bldr.setUserDefined((Type.UserDefined) o).build();
      }
      throw new UnsupportedOperationException("Unable to wrap type of " + o.getClass());
    }

    @Override
    protected Integer i(final int integerValue) {
      return integerValue;
    }
  }
}
