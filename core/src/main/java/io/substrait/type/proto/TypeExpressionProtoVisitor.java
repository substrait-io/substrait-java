package io.substrait.type.proto;

import io.substrait.extension.ExtensionCollector;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.proto.DerivationExpression;
import io.substrait.proto.DerivationExpression.ReturnProgram.Assignment;
import io.substrait.proto.Type;
import java.util.List;

public class TypeExpressionProtoVisitor
    extends BaseProtoConverter<DerivationExpression, DerivationExpression> {

  public TypeExpressionProtoVisitor(ExtensionCollector extensionCollector) {
    super(extensionCollector, "Unexpected expression type. This shouldn't happen.");
  }

  @Override
  public BaseProtoTypes<DerivationExpression, DerivationExpression> typeContainer(
      final boolean nullable) {
    return nullable ? DERIVATION_NULLABLE : DERIVATION_REQUIRED;
  }

  private static final DerivationTypes DERIVATION_NULLABLE =
      new DerivationTypes(Type.Nullability.NULLABILITY_NULLABLE);
  private static final DerivationTypes DERIVATION_REQUIRED =
      new DerivationTypes(Type.Nullability.NULLABILITY_REQUIRED);

  @Override
  public DerivationExpression visit(final TypeExpression.BinaryOperation expr) {
    DerivationExpression.BinaryOp.BinaryOpType opType = getDerivationOpType(expr.opType());
    return DerivationExpression.newBuilder()
        .setBinaryOp(
            DerivationExpression.BinaryOp.newBuilder()
                .setArg1(expr.left().accept(this))
                .setArg2(expr.right().accept(this))
                .setOpType(opType)
                .build())
        .build();
  }

  private DerivationExpression.BinaryOp.BinaryOpType getDerivationOpType(
      TypeExpression.BinaryOperation.OpType type) {
    switch (type) {
      case ADD:
        return DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_PLUS;
      case SUBTRACT:
        return DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MINUS;
      case MIN:
        return DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MIN;
      case MAX:
        return DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MAX;
      case LT:
        return DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_LESS_THAN;
      // case LTE -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_LESS_THAN;
      case GT:
        return DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_GREATER_THAN;
      // case GTE -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MINUS;
      // case NOT_EQ -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_EQ;
      case EQ:
        return DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_EQUALS;
      case COVERS:
        return DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_COVERS;
      default:
        throw new IllegalStateException("Unexpected value: " + type);
    }
  }

  @Override
  public DerivationExpression visit(final TypeExpression.NotOperation expr) {
    return DerivationExpression.newBuilder()
        .setUnaryOp(
            DerivationExpression.UnaryOp.newBuilder()
                .setOpType(DerivationExpression.UnaryOp.UnaryOpType.UNARY_OP_TYPE_BOOLEAN_NOT)
                .setArg(expr.inner().accept(this)))
        .build();
  }

  @Override
  public DerivationExpression visit(final TypeExpression.IfOperation expr) {
    return DerivationExpression.newBuilder()
        .setIfElse(
            DerivationExpression.IfElse.newBuilder()
                .setIfCondition(expr.ifCondition().accept(this))
                .setIfReturn(expr.thenExpr().accept(this))
                .setElseReturn(expr.elseExpr().accept(this))
                .build())
        .build();
  }

  @Override
  public DerivationExpression visit(final TypeExpression.IntegerLiteral expr) {
    return DerivationExpression.newBuilder().setIntegerLiteral(expr.value()).build();
  }

  @Override
  public DerivationExpression visit(final TypeExpression.ReturnProgram expr) {
    List<Assignment> assignments =
        expr.assignments().stream()
            .map(
                a ->
                    DerivationExpression.ReturnProgram.Assignment.newBuilder()
                        .setName(a.name())
                        .setExpression(a.expr().accept(this))
                        .build())
            .collect(java.util.stream.Collectors.toList());
    DerivationExpression finalExpr = expr.finalExpression().accept(this);
    return DerivationExpression.newBuilder()
        .setReturnProgram(
            DerivationExpression.ReturnProgram.newBuilder()
                .setFinalExpression(finalExpr)
                .addAllAssignments(assignments)
                .build())
        .build();
  }

  @Override
  public DerivationExpression visit(ParameterizedType.FixedChar expr) {
    return typeContainer(expr).fixedChar(expr.length().value());
  }

  @Override
  public DerivationExpression visit(ParameterizedType.VarChar expr) {
    return typeContainer(expr).varChar(expr.length().value());
  }

  @Override
  public DerivationExpression visit(ParameterizedType.FixedBinary expr) {
    return typeContainer(expr).fixedBinary(expr.length().value());
  }

  @Override
  public DerivationExpression visit(ParameterizedType.Decimal expr) {
    return typeContainer(expr).decimal(expr.precision().accept(this), expr.scale().accept(this));
  }

  @Override
  public DerivationExpression visit(ParameterizedType.IntervalDay expr) {
    return typeContainer(expr).intervalDay(expr.precision().accept(this));
  }

  @Override
  public DerivationExpression visit(ParameterizedType.IntervalCompound expr) {
    return typeContainer(expr).intervalCompound(expr.precision().accept(this));
  }

  @Override
  public DerivationExpression visit(ParameterizedType.PrecisionTimestamp expr) {
    return typeContainer(expr).precisionTimestamp(expr.precision().accept(this));
  }

  @Override
  public DerivationExpression visit(TypeExpression.PrecisionTimestampTZ expr) {
    return typeContainer(expr).precisionTimestampTZ(expr.precision().accept(this));
  }

  @Override
  public DerivationExpression visit(ParameterizedType.Struct expr) {
    return typeContainer(expr)
        .struct(
            expr.fields().stream()
                .map(f -> f.accept(this))
                .collect(java.util.stream.Collectors.toList()));
  }

  @Override
  public DerivationExpression visit(ParameterizedType.ListType expr) {
    return typeContainer(expr).list(expr.name().accept(this));
  }

  @Override
  public DerivationExpression visit(ParameterizedType.Map expr) {
    return typeContainer(expr).map(expr.key().accept(this), expr.value().accept(this));
  }

  @Override
  public DerivationExpression visit(ParameterizedType.StringLiteral stringLiteral) {
    return DerivationExpression.newBuilder().setTypeParameterName(stringLiteral.value()).build();
  }

  @Override
  public DerivationExpression visit(TypeExpression.FixedChar expr) {
    return typeContainer(expr).fixedChar(expr.length().accept(this));
  }

  @Override
  public DerivationExpression visit(TypeExpression.VarChar expr) {
    return typeContainer(expr).varChar(expr.length().accept(this));
  }

  @Override
  public DerivationExpression visit(TypeExpression.FixedBinary expr) {
    return typeContainer(expr).fixedBinary(expr.length().accept(this));
  }

  @Override
  public DerivationExpression visit(TypeExpression.Decimal expr) {
    return typeContainer(expr).decimal(expr.precision().accept(this), expr.scale().accept(this));
  }

  @Override
  public DerivationExpression visit(TypeExpression.Struct expr) {
    return typeContainer(expr)
        .struct(
            expr.fields().stream()
                .map(f -> f.accept(this))
                .collect(java.util.stream.Collectors.toList()));
  }

  @Override
  public DerivationExpression visit(TypeExpression.ListType expr) {
    return typeContainer(expr).list(expr.elementType().accept(this));
  }

  @Override
  public DerivationExpression visit(TypeExpression.Map expr) {
    return typeContainer(expr).map(expr.key().accept(this), expr.value().accept(this));
  }

  private static class DerivationTypes
      extends BaseProtoTypes<DerivationExpression, DerivationExpression> {

    public DerivationTypes(final Type.Nullability nullability) {
      super(nullability);
    }

    public DerivationExpression fixedChar(DerivationExpression len) {
      return wrap(
          DerivationExpression.ExpressionFixedChar.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression typeParam(final String name) {
      return DerivationExpression.newBuilder().setTypeParameterName(name).build();
    }

    @Override
    public DerivationExpression integerParam(final String name) {
      return DerivationExpression.newBuilder().setIntegerParameterName(name).build();
    }

    public DerivationExpression varChar(DerivationExpression len) {
      return wrap(
          DerivationExpression.ExpressionVarChar.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    public DerivationExpression fixedBinary(DerivationExpression len) {
      return wrap(
          DerivationExpression.ExpressionFixedBinary.newBuilder()
              .setLength(len)
              .setNullability(nullability)
              .build());
    }

    public DerivationExpression decimal(
        DerivationExpression scale, DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionDecimal.newBuilder()
              .setScale(scale)
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression precisionTime(DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionPrecisionTime.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression precisionTimestamp(DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionPrecisionTimestamp.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression precisionTimestampTZ(DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionPrecisionTimestampTZ.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression intervalDay(DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionIntervalDay.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    @Override
    public DerivationExpression intervalCompound(DerivationExpression precision) {
      return wrap(
          DerivationExpression.ExpressionIntervalCompound.newBuilder()
              .setPrecision(precision)
              .setNullability(nullability)
              .build());
    }

    public DerivationExpression struct(Iterable<DerivationExpression> types) {
      return wrap(
          DerivationExpression.ExpressionStruct.newBuilder()
              .addAllTypes(types)
              .setNullability(nullability)
              .build());
    }

    public DerivationExpression param(String name) {
      return DerivationExpression.newBuilder().setTypeParameterName(name).build();
    }

    public DerivationExpression list(DerivationExpression type) {
      return wrap(
          DerivationExpression.ExpressionList.newBuilder()
              .setType(type)
              .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
              .build());
    }

    public DerivationExpression map(DerivationExpression key, DerivationExpression value) {
      return wrap(
          DerivationExpression.ExpressionMap.newBuilder()
              .setKey(key)
              .setValue(value)
              .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
              .build());
    }

    @Override
    public DerivationExpression userDefined(int ref) {
      throw new UnsupportedOperationException(
          "User defined types are not supported in Derivation Expressions for now");
    }

    @Override
    protected DerivationExpression wrap(final Object o) {
      DerivationExpression.Builder bldr = DerivationExpression.newBuilder();
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
      } else if (o instanceof DerivationExpression.ExpressionIntervalDay) {
        return bldr.setIntervalDay((DerivationExpression.ExpressionIntervalDay) o).build();
      } else if (o instanceof DerivationExpression.ExpressionIntervalCompound) {
        return bldr.setIntervalCompound((DerivationExpression.ExpressionIntervalCompound) o)
            .build();
      } else if (o instanceof DerivationExpression.ExpressionFixedChar) {
        return bldr.setFixedChar((DerivationExpression.ExpressionFixedChar) o).build();
      } else if (o instanceof DerivationExpression.ExpressionVarChar) {
        return bldr.setVarchar((DerivationExpression.ExpressionVarChar) o).build();
      } else if (o instanceof DerivationExpression.ExpressionFixedBinary) {
        return bldr.setFixedBinary((DerivationExpression.ExpressionFixedBinary) o).build();
      } else if (o instanceof DerivationExpression.ExpressionDecimal) {
        return bldr.setDecimal((DerivationExpression.ExpressionDecimal) o).build();
      } else if (o instanceof DerivationExpression.ExpressionPrecisionTimestamp) {
        return bldr.setPrecisionTimestamp((DerivationExpression.ExpressionPrecisionTimestamp) o)
            .build();
      } else if (o instanceof DerivationExpression.ExpressionPrecisionTimestampTZ) {
        return bldr.setPrecisionTimestampTz((DerivationExpression.ExpressionPrecisionTimestampTZ) o)
            .build();
      } else if (o instanceof DerivationExpression.ExpressionStruct) {
        return bldr.setStruct((DerivationExpression.ExpressionStruct) o).build();
      } else if (o instanceof DerivationExpression.ExpressionList) {
        return bldr.setList((DerivationExpression.ExpressionList) o).build();
      } else if (o instanceof DerivationExpression.ExpressionMap) {
        return bldr.setMap((DerivationExpression.ExpressionMap) o).build();
      } else if (o instanceof Type.UUID) {
        return bldr.setUuid((Type.UUID) o).build();
      }
      throw new UnsupportedOperationException("Unable to wrap type of " + o.getClass());
    }

    @Override
    protected DerivationExpression i(final int integerValue) {
      return DerivationExpression.newBuilder().setIntegerLiteral(integerValue).build();
    }
  }
}
