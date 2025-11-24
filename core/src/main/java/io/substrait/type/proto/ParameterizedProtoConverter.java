package io.substrait.type.proto;

import io.substrait.extension.ExtensionCollector;
import io.substrait.function.TypeExpression;
import io.substrait.function.TypeExpressionVisitor;
import io.substrait.proto.ParameterizedType;
import io.substrait.proto.Type;

public class ParameterizedProtoConverter
    extends BaseProtoConverter<ParameterizedType, ParameterizedType.IntegerOption> {

  private static final BaseProtoTypes<ParameterizedType, ParameterizedType.IntegerOption>
      PARAMETERIZED_NULLABLE = new ParameterizedTypes(Type.Nullability.NULLABILITY_NULLABLE);
  private static final BaseProtoTypes<ParameterizedType, ParameterizedType.IntegerOption>
      PARAMETERIZED_REQUIRED = new ParameterizedTypes(Type.Nullability.NULLABILITY_REQUIRED);

  public ParameterizedProtoConverter(ExtensionCollector extensionCollector) {
    super(extensionCollector, "Parameterized types cannot include return type expressions.");
  }

  @Override
  public BaseProtoTypes<ParameterizedType, ParameterizedType.IntegerOption> typeContainer(
      final boolean nullable) {
    return nullable ? PARAMETERIZED_NULLABLE : PARAMETERIZED_REQUIRED;
  }

  public ParameterizedType.IntegerOption i(final TypeExpression num) {
    return num.accept(new IntegerVisitor());
  }

  @Override
  public ParameterizedType visit(io.substrait.function.ParameterizedType.FixedChar expr)
      throws RuntimeException {
    return typeContainer(expr).fixedChar(expr.length().value());
  }

  @Override
  public ParameterizedType visit(io.substrait.function.ParameterizedType.VarChar expr)
      throws RuntimeException {
    return typeContainer(expr).varChar(expr.length().value());
  }

  @Override
  public ParameterizedType visit(io.substrait.function.ParameterizedType.FixedBinary expr)
      throws RuntimeException {
    return typeContainer(expr).fixedBinary(expr.length().value());
  }

  @Override
  public ParameterizedType visit(io.substrait.function.ParameterizedType.Decimal expr)
      throws RuntimeException {
    return typeContainer(expr).decimal(i(expr.precision()), i(expr.scale()));
  }

  @Override
  public ParameterizedType visit(io.substrait.function.ParameterizedType.PrecisionTimestamp expr)
      throws RuntimeException {
    return typeContainer(expr).precisionTimestamp(i(expr.precision()));
  }

  @Override
  public ParameterizedType visit(io.substrait.function.ParameterizedType.PrecisionTimestampTZ expr)
      throws RuntimeException {
    return typeContainer(expr).precisionTimestampTZ(i(expr.precision()));
  }

  @Override
  public ParameterizedType visit(io.substrait.function.ParameterizedType.Struct expr)
      throws RuntimeException {
    return typeContainer(expr)
        .struct(
            expr.fields().stream()
                .map(f -> f.accept(this))
                .collect(java.util.stream.Collectors.toList()));
  }

  @Override
  public ParameterizedType visit(io.substrait.function.ParameterizedType.ListType expr)
      throws RuntimeException {
    return typeContainer(expr).list(expr.name().accept(this));
  }

  @Override
  public ParameterizedType visit(io.substrait.function.ParameterizedType.Map expr)
      throws RuntimeException {
    return typeContainer(expr).map(expr.key().accept(this), expr.value().accept(this));
  }

  @Override
  public ParameterizedType visit(
      io.substrait.function.ParameterizedType.StringLiteral stringLiteral) throws RuntimeException {
    return ParameterizedType.newBuilder()
        .setTypeParameter(
            ParameterizedType.TypeParameter.newBuilder().setName(stringLiteral.value()))
        .build();
  }

  private static class IntegerVisitor
      extends TypeExpressionVisitor.TypeExpressionThrowsVisitor<
          ParameterizedType.IntegerOption, RuntimeException> {

    public IntegerVisitor() {
      super("Parameterized integer locations should only include integer names or literals.");
    }

    @Override
    public ParameterizedType.IntegerOption visit(final TypeExpression.IntegerLiteral literal) {
      return ParameterizedType.IntegerOption.newBuilder().setLiteral(literal.value()).build();
    }

    @Override
    public ParameterizedType.IntegerOption visit(
        io.substrait.function.ParameterizedType.StringLiteral stringLiteral)
        throws RuntimeException {
      return ParameterizedType.IntegerOption.newBuilder()
          .setParameter(
              ParameterizedType.IntegerParameter.newBuilder().setName(stringLiteral.value()))
          .build();
    }
  }

  public static class ParameterizedTypes
      extends BaseProtoTypes<ParameterizedType, ParameterizedType.IntegerOption> {

    public ParameterizedTypes(final Type.Nullability nullability) {
      super(nullability);
    }

    @Override
    public ParameterizedType fixedChar(ParameterizedType.IntegerOption len) {
      return wrap(
          ParameterizedType.ParameterizedFixedChar.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType typeParam(final String name) {
      return ParameterizedType.newBuilder()
          .setTypeParameter(ParameterizedType.TypeParameter.newBuilder().setName(name))
          .build();
    }

    @Override
    public ParameterizedType.IntegerOption integerParam(final String name) {
      return ParameterizedType.IntegerOption.newBuilder()
          .setParameter(ParameterizedType.IntegerParameter.newBuilder().setName(name))
          .build();
    }

    @Override
    protected ParameterizedType.IntegerOption i(int len) {
      return ParameterizedType.IntegerOption.newBuilder().setLiteral(len).build();
    }

    @Override
    public ParameterizedType varChar(ParameterizedType.IntegerOption len) {
      return wrap(
          ParameterizedType.ParameterizedVarChar.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType fixedBinary(ParameterizedType.IntegerOption len) {
      return wrap(
          ParameterizedType.ParameterizedFixedBinary.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType decimal(
        ParameterizedType.IntegerOption scale, ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedDecimal.newBuilder()
              .setScale(scale)
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType intervalDay(ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedIntervalDay.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType intervalCompound(ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedIntervalCompound.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType precisionTime(ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedPrecisionTime.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType precisionTimestamp(ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedPrecisionTimestamp.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType precisionTimestampTZ(ParameterizedType.IntegerOption precision) {
      return wrap(
          ParameterizedType.ParameterizedPrecisionTimestampTZ.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType struct(Iterable<ParameterizedType> types) {
      return wrap(
          ParameterizedType.ParameterizedStruct.newBuilder()
              .addAllTypes(types)
              .setNullability(nullability)
              .build());
    }

    @Override
    public ParameterizedType list(ParameterizedType type) {
      return wrap(
          ParameterizedType.ParameterizedList.newBuilder()
              .setType(type)
              .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
              .build());
    }

    @Override
    public ParameterizedType map(ParameterizedType key, ParameterizedType value) {
      return wrap(
          ParameterizedType.ParameterizedMap.newBuilder()
              .setKey(key)
              .setValue(value)
              .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
              .build());
    }

    @Override
    public ParameterizedType userDefined(int ref) {
      throw new UnsupportedOperationException(
          "User defined types are not supported in Parameterized Types for now");
    }

    @Override
    public ParameterizedType userDefined(
        int ref, java.util.List<io.substrait.type.Type.Parameter> typeParameters) {
      throw new UnsupportedOperationException(
          "User defined types are not supported in Parameterized Types for now");
    }

    @Override
    protected ParameterizedType wrap(final Object o) {
      ParameterizedType.Builder bldr = ParameterizedType.newBuilder();
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
      } else if (o instanceof ParameterizedType.ParameterizedIntervalDay) {
        return bldr.setIntervalDay((ParameterizedType.ParameterizedIntervalDay) o).build();
      } else if (o instanceof ParameterizedType.ParameterizedIntervalCompound) {
        return bldr.setIntervalCompound((ParameterizedType.ParameterizedIntervalCompound) o)
            .build();
      } else if (o instanceof ParameterizedType.ParameterizedFixedChar) {
        return bldr.setFixedChar((ParameterizedType.ParameterizedFixedChar) o).build();
      } else if (o instanceof ParameterizedType.ParameterizedVarChar) {
        return bldr.setVarchar((ParameterizedType.ParameterizedVarChar) o).build();
      } else if (o instanceof ParameterizedType.ParameterizedFixedBinary) {
        return bldr.setFixedBinary((ParameterizedType.ParameterizedFixedBinary) o).build();
      } else if (o instanceof ParameterizedType.ParameterizedDecimal) {
        return bldr.setDecimal((ParameterizedType.ParameterizedDecimal) o).build();
      } else if (o instanceof ParameterizedType.ParameterizedPrecisionTimestamp) {
        return bldr.setPrecisionTimestamp((ParameterizedType.ParameterizedPrecisionTimestamp) o)
            .build();
      } else if (o instanceof ParameterizedType.ParameterizedPrecisionTimestampTZ) {
        return bldr.setPrecisionTimestampTz((ParameterizedType.ParameterizedPrecisionTimestampTZ) o)
            .build();
      } else if (o instanceof ParameterizedType.ParameterizedStruct) {
        return bldr.setStruct((ParameterizedType.ParameterizedStruct) o).build();
      } else if (o instanceof ParameterizedType.ParameterizedList) {
        return bldr.setList((ParameterizedType.ParameterizedList) o).build();
      } else if (o instanceof ParameterizedType.ParameterizedMap) {
        return bldr.setMap((ParameterizedType.ParameterizedMap) o).build();
      } else if (o instanceof Type.UUID) {
        return bldr.setUuid((Type.UUID) o).build();
      }
      throw new UnsupportedOperationException("Unable to wrap type of " + o.getClass());
    }
  }
}
