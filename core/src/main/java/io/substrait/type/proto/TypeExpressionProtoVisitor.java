package io.substrait.type.proto;

import io.substrait.expression.proto.FunctionCollector;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.proto.DerivationExpression;
import io.substrait.proto.Type;

public class TypeExpressionProtoVisitor
    extends BaseProtoConverter<DerivationExpression, DerivationExpression> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TypeExpressionProtoVisitor.class);

  public TypeExpressionProtoVisitor(FunctionCollector extensionCollector) {
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
    var opType =
        switch (expr.opType()) {
          case ADD -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_PLUS;
          case SUBTRACT -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MINUS;
          case MIN -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MIN;
          case MAX -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MAX;
          case LT -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_LESS_THAN;
            // case LTE -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_LESS_THAN;
          case GT -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_GREATER_THAN;
            // case GTE -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MINUS;
            // case NOT_EQ -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_EQ;
          case EQ -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_EQUALS;
          case COVERS -> DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_COVERS;
          default -> throw new IllegalStateException("Unexpected value: " + expr.opType());
        };
    return DerivationExpression.newBuilder()
        .setBinaryOp(
            DerivationExpression.BinaryOp.newBuilder()
                .setArg1(expr.left().accept(this))
                .setArg2(expr.right().accept(this))
                .setOpType(opType)
                .build())
        .build();
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
    var assignments =
        expr.assignments().stream()
            .map(
                a ->
                    DerivationExpression.ReturnProgram.Assignment.newBuilder()
                        .setName(a.name())
                        .setExpression(a.expr().accept(this))
                        .build())
            .collect(java.util.stream.Collectors.toList());
    var finalExpr = expr.finalExpression().accept(this);
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
      var bldr = DerivationExpression.newBuilder();
      if (o instanceof Type.Boolean t) {
        return bldr.setBool(t).build();
      } else if (o instanceof Type.I8 t) {
        return bldr.setI8(t).build();
      } else if (o instanceof Type.I16 t) {
        return bldr.setI16(t).build();
      } else if (o instanceof Type.I32 t) {
        return bldr.setI32(t).build();
      } else if (o instanceof Type.I64 t) {
        return bldr.setI64(t).build();
      } else if (o instanceof Type.FP32 t) {
        return bldr.setFp32(t).build();
      } else if (o instanceof Type.FP64 t) {
        return bldr.setFp64(t).build();
      } else if (o instanceof Type.String t) {
        return bldr.setString(t).build();
      } else if (o instanceof Type.Binary t) {
        return bldr.setBinary(t).build();
      } else if (o instanceof Type.Timestamp t) {
        return bldr.setTimestamp(t).build();
      } else if (o instanceof Type.Date t) {
        return bldr.setDate(t).build();
      } else if (o instanceof Type.Time t) {
        return bldr.setTime(t).build();
      } else if (o instanceof Type.TimestampTZ t) {
        return bldr.setTimestampTz(t).build();
      } else if (o instanceof Type.IntervalYear t) {
        return bldr.setIntervalYear(t).build();
      } else if (o instanceof Type.IntervalDay t) {
        return bldr.setIntervalDay(t).build();
      } else if (o instanceof DerivationExpression.ExpressionFixedChar t) {
        return bldr.setFixedChar(t).build();
      } else if (o instanceof DerivationExpression.ExpressionVarChar t) {
        return bldr.setVarchar(t).build();
      } else if (o instanceof DerivationExpression.ExpressionFixedBinary t) {
        return bldr.setFixedBinary(t).build();
      } else if (o instanceof DerivationExpression.ExpressionDecimal t) {
        return bldr.setDecimal(t).build();
      } else if (o instanceof DerivationExpression.ExpressionStruct t) {
        return bldr.setStruct(t).build();
      } else if (o instanceof DerivationExpression.ExpressionList t) {
        return bldr.setList(t).build();
      } else if (o instanceof DerivationExpression.ExpressionMap t) {
        return bldr.setMap(t).build();
      } else if (o instanceof Type.UUID t) {
        return bldr.setUuid(t).build();
      }
      throw new UnsupportedOperationException("Unable to wrap type of " + o.getClass());
    }

    @Override
    protected DerivationExpression i(final int integerValue) {
      return DerivationExpression.newBuilder().setIntegerLiteral(integerValue).build();
    }
  }
}
