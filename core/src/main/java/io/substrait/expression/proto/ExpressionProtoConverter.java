package io.substrait.expression.proto;

import io.substrait.expression.AbstractExpressionVisitor;
import io.substrait.expression.ExpressionVisitor;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.Expression;
import io.substrait.proto.Rel;
import io.substrait.proto.SortField;
import io.substrait.relation.RelVisitor;
import io.substrait.type.proto.TypeProtoConverter;
import java.util.List;
import java.util.function.Consumer;

/**
 * Converts from {@link io.substrait.expression.Expression} to {@link io.substrait.proto.Expression}
 */
public class ExpressionProtoConverter implements ExpressionVisitor<Expression, RuntimeException> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ExpressionProtoConverter.class);

  private final ExtensionCollector extensionCollector;
  private final RelVisitor<Rel, RuntimeException> relVisitor;
  private final TypeProtoConverter typeProtoConverter;

  public ExpressionProtoConverter(
      ExtensionCollector extensionCollector, RelVisitor<Rel, RuntimeException> relVisitor) {
    this.extensionCollector = extensionCollector;
    this.relVisitor = relVisitor;
    this.typeProtoConverter = new TypeProtoConverter(extensionCollector);
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.NullLiteral expr) {
    return lit(bldr -> bldr.setNull(expr.type().accept(typeProtoConverter)));
  }

  private Expression lit(Consumer<Expression.Literal.Builder> consumer) {
    var builder = Expression.Literal.newBuilder();
    consumer.accept(builder);
    return Expression.newBuilder().setLiteral(builder).build();
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.BoolLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setBoolean(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.I8Literal expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setI8(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.I16Literal expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setI16(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.I32Literal expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setI32(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.I64Literal expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setI64(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.FP32Literal expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setFp32(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.FP64Literal expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setFp64(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.StrLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setString(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.BinaryLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setBinary(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.TimeLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setTime(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.DateLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setDate(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.TimestampLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setTimestamp(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.TimestampTZLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setTimestampTz(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.IntervalYearLiteral expr) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setIntervalYearToMonth(
                    Expression.Literal.IntervalYearToMonth.newBuilder()
                        .setYears(expr.years())
                        .setMonths(expr.months())));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.IntervalDayLiteral expr) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setIntervalDayToSecond(
                    Expression.Literal.IntervalDayToSecond.newBuilder()
                        .setDays(expr.days())
                        .setSeconds(expr.seconds())));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.UUIDLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setUuid(expr.toBytes()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.FixedCharLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setFixedChar(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.VarCharLiteral expr) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setVarChar(
                    Expression.Literal.VarChar.newBuilder()
                        .setValue(expr.value())
                        .setLength(expr.length())));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.FixedBinaryLiteral expr) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setFixedBinary(expr.value()));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.DecimalLiteral expr) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setDecimal(
                    Expression.Literal.Decimal.newBuilder()
                        .setValue(expr.value())
                        .setPrecision(expr.precision())
                        .setScale(expr.scale())));
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.MapLiteral expr) {
    return lit(
        bldr -> {
          var keyValues =
              expr.values().entrySet().stream()
                  .map(
                      e -> {
                        var key = toLiteral(e.getKey());
                        var value = toLiteral(e.getValue());
                        return Expression.Literal.Map.KeyValue.newBuilder()
                            .setKey(key)
                            .setValue(value)
                            .build();
                      })
                  .collect(java.util.stream.Collectors.toList());
          bldr.setNullable(expr.nullable())
              .setMap(Expression.Literal.Map.newBuilder().addAllKeyValues(keyValues));
        });
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.ListLiteral expr) {
    return lit(
        bldr -> {
          var values =
              expr.values().stream()
                  .map(this::toLiteral)
                  .collect(java.util.stream.Collectors.toList());
          bldr.setNullable(expr.nullable())
              .setList(Expression.Literal.List.newBuilder().addAllValues(values));
        });
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.StructLiteral expr) {
    return lit(
        bldr -> {
          var values =
              expr.fields().stream()
                  .map(this::toLiteral)
                  .collect(java.util.stream.Collectors.toList());
          bldr.setNullable(expr.nullable())
              .setStruct(Expression.Literal.Struct.newBuilder().addAllFields(values));
        });
  }

  private Expression.Literal toLiteral(io.substrait.expression.Expression expression) {
    var e = expression.accept(this);
    assert e.getRexTypeCase() == Expression.RexTypeCase.LITERAL;
    return e.getLiteral();
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.Switch expr) {
    var clauses =
        expr.switchClauses().stream()
            .map(
                s ->
                    Expression.SwitchExpression.IfValue.newBuilder()
                        .setIf(toLiteral(s.condition()))
                        .setThen(s.then().accept(this))
                        .build())
            .collect(java.util.stream.Collectors.toList());
    return Expression.newBuilder()
        .setSwitchExpression(
            Expression.SwitchExpression.newBuilder()
                .addAllIfs(clauses)
                .setElse(expr.defaultClause().accept(this)))
        .build();
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.IfThen expr) {
    var clauses =
        expr.ifClauses().stream()
            .map(
                s ->
                    Expression.IfThen.IfClause.newBuilder()
                        .setIf(s.condition().accept(this))
                        .setThen(s.then().accept(this))
                        .build())
            .collect(java.util.stream.Collectors.toList());
    return Expression.newBuilder()
        .setIfThen(
            Expression.IfThen.newBuilder()
                .addAllIfs(clauses)
                .setElse(expr.elseClause().accept(this)))
        .build();
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.ScalarFunctionInvocation expr) {

    var argVisitor = FunctionArg.toProto(typeProtoConverter, this);

    return Expression.newBuilder()
        .setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setOutputType(expr.getType().accept(typeProtoConverter))
                .setFunctionReference(extensionCollector.getFunctionReference(expr.declaration()))
                .addAllArguments(
                    expr.arguments().stream()
                        .map(a -> a.accept(expr.declaration(), 0, argVisitor))
                        .collect(java.util.stream.Collectors.toList())))
        .build();
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.Cast expr) {
    return Expression.newBuilder()
        .setCast(
            Expression.Cast.newBuilder()
                .setInput(expr.input().accept(this))
                .setType(expr.getType().accept(typeProtoConverter))
                .setFailureBehavior(expr.failureBehavior().toProto()))
        .build();
  }

  private Expression from(io.substrait.expression.Expression expr) {
    return expr.accept(this);
  }

  private List<Expression> from(List<io.substrait.expression.Expression> expr) {
    return expr.stream().map(this::from).collect(java.util.stream.Collectors.toList());
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.SingleOrList expr)
      throws RuntimeException {
    return Expression.newBuilder()
        .setSingularOrList(
            Expression.SingularOrList.newBuilder()
                .setValue(expr.condition().accept(this))
                .addAllOptions(from(expr.options())))
        .build();
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.MultiOrList expr)
      throws RuntimeException {
    return Expression.newBuilder()
        .setMultiOrList(
            Expression.MultiOrList.newBuilder()
                .addAllValue(from(expr.conditions()))
                .addAllOptions(
                    expr.optionCombinations().stream()
                        .map(
                            r ->
                                Expression.MultiOrList.Record.newBuilder()
                                    .addAllFields(from(r.values()))
                                    .build())
                        .collect(java.util.stream.Collectors.toList())))
        .build();
  }

  @Override
  public Expression visit(FieldReference expr) {
    Expression.ReferenceSegment top = null;
    Expression.ReferenceSegment seg = null;
    for (var segment : expr.segments()) {
      Expression.ReferenceSegment.Builder protoSegment;
      if (segment instanceof FieldReference.StructField f) {
        var bldr = Expression.ReferenceSegment.StructField.newBuilder().setField(f.offset());
        if (seg != null) {
          bldr.setChild(seg);
        }
        protoSegment = Expression.ReferenceSegment.newBuilder().setStructField(bldr);
      } else if (segment instanceof FieldReference.ListElement f) {
        var bldr = Expression.ReferenceSegment.ListElement.newBuilder().setOffset(f.offset());
        if (seg != null) {
          bldr.setChild(seg);
        }
        protoSegment = Expression.ReferenceSegment.newBuilder().setListElement(bldr);
      } else if (segment instanceof FieldReference.MapKey f) {
        var bldr = Expression.ReferenceSegment.MapKey.newBuilder().setMapKey(toLiteral(f.key()));
        if (seg != null) {
          bldr.setChild(seg);
        }
        protoSegment = Expression.ReferenceSegment.newBuilder().setMapKey(bldr);
      } else {
        throw new IllegalArgumentException("Unhandled type: " + segment);
      }
      var builtSegment = protoSegment.build();
      if (top == null) {
        top = builtSegment;
      }
      seg = builtSegment;
    }

    var out = Expression.FieldReference.newBuilder().setDirectReference(top);
    if (expr.inputExpression().isPresent()) {
      out.setExpression(from(expr.inputExpression().get()));
    } else if (expr.outerReferenceStepsOut().isPresent()) {
      out.setOuterReference(
          io.substrait.proto.Expression.FieldReference.OuterReference.newBuilder()
              .setStepsOut(expr.outerReferenceStepsOut().get()));
    } else {
      out.setRootReference(Expression.FieldReference.RootReference.getDefaultInstance());
    }

    return Expression.newBuilder().setSelection(out).build();
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.SetPredicate expr)
      throws RuntimeException {
    return Expression.newBuilder()
        .setSubquery(
            Expression.Subquery.newBuilder()
                .setSetPredicate(
                    Expression.Subquery.SetPredicate.newBuilder()
                        .setPredicateOp(expr.predicateOp().toProto())
                        .setTuples(expr.tuples().accept(this.relVisitor))
                        .build())
                .build())
        .build();
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.ScalarSubquery expr)
      throws RuntimeException {
    return Expression.newBuilder()
        .setSubquery(
            Expression.Subquery.newBuilder()
                .setScalar(
                    Expression.Subquery.Scalar.newBuilder()
                        .setInput(expr.input().accept(this.relVisitor))
                        .build())
                .build())
        .build();
  }

  @Override
  public Expression visit(io.substrait.expression.Expression.InPredicate expr)
      throws RuntimeException {
    return Expression.newBuilder()
        .setSubquery(
            Expression.Subquery.newBuilder()
                .setInPredicate(
                    Expression.Subquery.InPredicate.newBuilder()
                        .setHaystack(expr.haystack().accept(this.relVisitor))
                        .addAllNeedles(from(expr.needles()))
                        .build())
                .build())
        .build();
  }

  public Expression visit(io.substrait.expression.Expression.Window expr) throws RuntimeException {
    var partExps =
        expr.partitionBy().stream()
            .map(e -> e.accept(this))
            .collect(java.util.stream.Collectors.toList());
    var outputType = expr.getType().accept(typeProtoConverter);
    var builder = Expression.WindowFunction.newBuilder().setOutputType(outputType);
    if (expr.hasNormalAggregateFunction()) {
      var aggMeasureFunc = expr.aggregateFunction().getFunction();
      var funcReference = extensionCollector.getFunctionReference(aggMeasureFunc.declaration());
      var argVisitor = FunctionArg.toProto(typeProtoConverter, this);
      var args =
          aggMeasureFunc.arguments().stream()
              .map(a -> a.accept(aggMeasureFunc.declaration(), 0, argVisitor))
              .collect(java.util.stream.Collectors.toList());
      builder
          .setFunctionReference(funcReference)
          .setPhase(aggMeasureFunc.aggregationPhase().toProto())
          .addAllArguments(args);
    } else {
      var windowFunc = expr.windowFunction().getFunction();
      var funcReference = extensionCollector.getFunctionReference(windowFunc.declaration());
      var argVisitor = FunctionArg.toProto(typeProtoConverter, this);
      var args =
          windowFunc.arguments().stream()
              .map(a -> a.accept(windowFunc.declaration(), 0, argVisitor))
              .collect(java.util.stream.Collectors.toList());
      builder
          .setFunctionReference(funcReference)
          .setPhase(windowFunc.aggregationPhase().toProto())
          .addAllArguments(args);
    }
    var sortFields =
        expr.orderBy().stream()
            .map(
                s -> {
                  return SortField.newBuilder()
                      .setDirection(s.direction().toProto())
                      .setExpr(s.expr().accept(this))
                      .build();
                })
            .collect(java.util.stream.Collectors.toList());
    var upperBound = toBound(expr.upperBound());
    var lowerBound = toBound(expr.lowerBound());
    return Expression.newBuilder()
        .setWindowFunction(
            builder
                .addAllPartitions(partExps)
                .addAllSorts(sortFields)
                .setLowerBound(lowerBound)
                .setUpperBound(upperBound)
                .build())
        .build();
  }

  private static class LiteralToWindowBoundOffset
      extends AbstractExpressionVisitor<Long, RuntimeException> {

    @Override
    public Long visitFallback(io.substrait.expression.Expression expr) {
      throw new RuntimeException(
          String.format("Expected positive integer for Window Bound offset, received: %s", expr));
    }

    private static long offsetIsPositive(long offset) {
      if (offset >= 1) {
        return offset;
      }
      throw new RuntimeException(
          String.format("Expected positive offset for Window Bound offset, recieved: %d", offset));
    }

    @Override
    public Long visit(io.substrait.expression.Expression.I8Literal expr) throws RuntimeException {
      return offsetIsPositive(expr.value());
    }

    @Override
    public Long visit(io.substrait.expression.Expression.I16Literal expr) throws RuntimeException {
      return offsetIsPositive(expr.value());
    }

    @Override
    public Long visit(io.substrait.expression.Expression.I32Literal expr) throws RuntimeException {
      return offsetIsPositive(expr.value());
    }

    @Override
    public Long visit(io.substrait.expression.Expression.I64Literal expr) throws RuntimeException {
      return offsetIsPositive(expr.value());
    }
  }

  private Expression.WindowFunction.Bound toBound(io.substrait.expression.WindowBound windowBound) {
    var boundedKind = windowBound.boundedKind();
    return switch (boundedKind) {
      case CURRENT_ROW -> Expression.WindowFunction.Bound.newBuilder()
          .setCurrentRow(Expression.WindowFunction.Bound.CurrentRow.getDefaultInstance())
          .build();
      case BOUNDED -> {
        WindowBound.BoundedWindowBound boundedWindowBound =
            (WindowBound.BoundedWindowBound) windowBound;
        var offset = boundedWindowBound.offset().accept(new LiteralToWindowBoundOffset());
        yield switch (boundedWindowBound.direction()) {
          case PRECEDING -> Expression.WindowFunction.Bound.newBuilder()
              .setPreceding(
                  Expression.WindowFunction.Bound.Preceding.newBuilder().setOffset(offset))
              .build();
          case FOLLOWING -> Expression.WindowFunction.Bound.newBuilder()
              .setFollowing(
                  Expression.WindowFunction.Bound.Following.newBuilder().setOffset(offset))
              .build();
        };
      }
      case UNBOUNDED -> Expression.WindowFunction.Bound.newBuilder()
          .setUnbounded(Expression.WindowFunction.Bound.Unbounded.getDefaultInstance())
          .build();
    };
  }
}
