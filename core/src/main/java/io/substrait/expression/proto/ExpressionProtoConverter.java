package io.substrait.expression.proto;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.expression.ExpressionVisitor;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.FunctionOption;
import io.substrait.proto.SortField;
import io.substrait.proto.Type;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.proto.TypeProtoConverter;
import io.substrait.util.EmptyVisitationContext;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Converts from {@link io.substrait.expression.Expression} to {@link io.substrait.proto.Expression}
 */
public class ExpressionProtoConverter
    implements ExpressionVisitor<Expression, EmptyVisitationContext, RuntimeException> {

  protected final RelProtoConverter relProtoConverter;
  protected final TypeProtoConverter typeProtoConverter;

  protected final ExtensionCollector extensionCollector;

  public ExpressionProtoConverter(
      ExtensionCollector extensionCollector, RelProtoConverter relProtoConverter) {
    this.extensionCollector = extensionCollector;
    this.relProtoConverter = relProtoConverter;
    this.typeProtoConverter = new TypeProtoConverter(extensionCollector);
  }

  public RelProtoConverter getRelProtoConverter() {
    return this.relProtoConverter;
  }

  public TypeProtoConverter getTypeProtoConverter() {
    return this.typeProtoConverter;
  }

  public io.substrait.proto.Expression toProto(io.substrait.expression.Expression expression) {
    return expression.accept(this, EmptyVisitationContext.INSTANCE);
  }

  public List<io.substrait.proto.Expression> toProto(
      List<io.substrait.expression.Expression> expressions) {
    return expressions.stream().map(this::toProto).collect(Collectors.toList());
  }

  protected io.substrait.proto.Rel toProto(io.substrait.relation.Rel rel) {
    return relProtoConverter.toProto(rel);
  }

  protected io.substrait.proto.Type toProto(io.substrait.type.Type type) {
    return typeProtoConverter.toProto(type);
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.NullLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNull(toProto(expr.type())));
  }

  private Expression lit(Consumer<Expression.Literal.Builder> consumer) {
    Expression.Literal.Builder builder = Expression.Literal.newBuilder();
    consumer.accept(builder);
    return Expression.newBuilder().setLiteral(builder).build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.BoolLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setBoolean(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.I8Literal expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setI8(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.I16Literal expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setI16(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.I32Literal expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setI32(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.I64Literal expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setI64(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.FP32Literal expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setFp32(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.FP64Literal expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setFp64(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.StrLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setString(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.BinaryLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setBinary(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.TimeLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setTime(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.DateLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setDate(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.TimestampLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setTimestamp(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.TimestampTZLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setTimestampTz(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.PrecisionTimestampLiteral expr,
      EmptyVisitationContext context) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setPrecisionTimestamp(
                    Expression.Literal.PrecisionTimestamp.newBuilder()
                        .setValue(expr.value())
                        .setPrecision(expr.precision())
                        .build())
                .build());
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.PrecisionTimestampTZLiteral expr,
      EmptyVisitationContext context) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setPrecisionTimestampTz(
                    Expression.Literal.PrecisionTimestamp.newBuilder()
                        .setValue(expr.value())
                        .setPrecision(expr.precision())
                        .build())
                .build());
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.IntervalYearLiteral expr, EmptyVisitationContext context) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setIntervalYearToMonth(
                    Expression.Literal.IntervalYearToMonth.newBuilder()
                        .setYears(expr.years())
                        .setMonths(expr.months())));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.IntervalDayLiteral expr, EmptyVisitationContext context) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setIntervalDayToSecond(
                    Expression.Literal.IntervalDayToSecond.newBuilder()
                        .setDays(expr.days())
                        .setSeconds(expr.seconds())
                        .setSubseconds(expr.subseconds())
                        .setPrecision(expr.precision())));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.IntervalCompoundLiteral expr,
      EmptyVisitationContext context) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setIntervalCompound(
                    Expression.Literal.IntervalCompound.newBuilder()
                        .setIntervalYearToMonth(
                            Expression.Literal.IntervalYearToMonth.newBuilder()
                                .setYears(expr.years())
                                .setMonths(expr.months()))
                        .setIntervalDayToSecond(
                            Expression.Literal.IntervalDayToSecond.newBuilder()
                                .setDays(expr.days())
                                .setSeconds(expr.seconds())
                                .setSubseconds(expr.subseconds())
                                .setPrecision(expr.precision()))));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.UUIDLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setUuid(expr.toBytes()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.FixedCharLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setFixedChar(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.VarCharLiteral expr, EmptyVisitationContext context) {
    return lit(
        bldr ->
            bldr.setNullable(expr.nullable())
                .setVarChar(
                    Expression.Literal.VarChar.newBuilder()
                        .setValue(expr.value())
                        .setLength(expr.length())));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.FixedBinaryLiteral expr, EmptyVisitationContext context) {
    return lit(bldr -> bldr.setNullable(expr.nullable()).setFixedBinary(expr.value()));
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.DecimalLiteral expr, EmptyVisitationContext context) {
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
  public Expression visit(
      io.substrait.expression.Expression.MapLiteral expr, EmptyVisitationContext context) {
    return lit(
        bldr -> {
          List<Expression.Literal.Map.KeyValue> keyValues =
              expr.values().entrySet().stream()
                  .map(
                      e -> {
                        Expression.Literal key = toLiteral(e.getKey());
                        Expression.Literal value = toLiteral(e.getValue());
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
  public Expression visit(
      io.substrait.expression.Expression.EmptyMapLiteral expr, EmptyVisitationContext context) {
    return lit(
        bldr -> {
          Type protoMapType = toProto(expr.getType());
          bldr.setEmptyMap(protoMapType.getMap())
              // For empty maps, the Literal message's own nullable field should be ignored
              // in favor of the nullability of the Type.Map in the literal's
              // empty_map field. But for safety we set the literal's nullable field
              // to match in case any readers either look in the wrong location
              // or want to verify that they are consistent.
              .setNullable(expr.nullable());
        });
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.ListLiteral expr, EmptyVisitationContext context) {
    return lit(
        bldr -> {
          List<Expression.Literal> values =
              expr.values().stream()
                  .map(this::toLiteral)
                  .collect(java.util.stream.Collectors.toList());
          bldr.setNullable(expr.nullable())
              .setList(Expression.Literal.List.newBuilder().addAllValues(values));
        });
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.EmptyListLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return lit(
        builder -> {
          Type protoListType = toProto(expr.getType());
          builder
              .setEmptyList(protoListType.getList())
              // For empty lists, the Literal message's own nullable field should be ignored
              // in favor of the nullability of the Type.List in the literal's
              // empty_list field. But for safety we set the literal's nullable field
              // to match in case any readers either look in the wrong location
              // or want to verify that they are consistent.
              .setNullable(expr.nullable());
        });
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.StructLiteral expr, EmptyVisitationContext context) {
    return lit(
        bldr -> {
          List<Expression.Literal> values =
              expr.fields().stream()
                  .map(this::toLiteral)
                  .collect(java.util.stream.Collectors.toList());
          bldr.setNullable(expr.nullable())
              .setStruct(Expression.Literal.Struct.newBuilder().addAllFields(values));
        });
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.UserDefinedLiteral expr, EmptyVisitationContext context) {
    int typeReference =
        extensionCollector.getTypeReference(SimpleExtension.TypeAnchor.of(expr.urn(), expr.name()));
    return lit(
        bldr -> {
          try {
            bldr.setNullable(expr.nullable())
                .setUserDefined(
                    Expression.Literal.UserDefined.newBuilder()
                        .setTypeReference(typeReference)
                        .setValue(Any.parseFrom(expr.value())))
                .build();
          } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
          }
        });
  }

  private Expression.Literal toLiteral(io.substrait.expression.Expression expression) {
    Expression e = toProto(expression);
    assert e.getRexTypeCase() == Expression.RexTypeCase.LITERAL;
    return e.getLiteral();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.Switch expr, EmptyVisitationContext context) {
    List<Expression.SwitchExpression.IfValue> clauses =
        expr.switchClauses().stream()
            .map(
                s ->
                    Expression.SwitchExpression.IfValue.newBuilder()
                        .setIf(toLiteral(s.condition()))
                        .setThen(toProto(s.then()))
                        .build())
            .collect(java.util.stream.Collectors.toList());
    return Expression.newBuilder()
        .setSwitchExpression(
            Expression.SwitchExpression.newBuilder()
                .setMatch(toProto(expr.match()))
                .addAllIfs(clauses)
                .setElse(toProto(expr.defaultClause())))
        .build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.IfThen expr, EmptyVisitationContext context) {
    List<Expression.IfThen.IfClause> clauses =
        expr.ifClauses().stream()
            .map(
                s ->
                    Expression.IfThen.IfClause.newBuilder()
                        .setIf(toProto(s.condition()))
                        .setThen(toProto(s.then()))
                        .build())
            .collect(java.util.stream.Collectors.toList());
    return Expression.newBuilder()
        .setIfThen(
            Expression.IfThen.newBuilder().addAllIfs(clauses).setElse(toProto(expr.elseClause())))
        .build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.ScalarFunctionInvocation expr,
      EmptyVisitationContext context) {

    FunctionArg.FuncArgVisitor<FunctionArgument, EmptyVisitationContext, RuntimeException>
        argVisitor = FunctionArg.toProto(typeProtoConverter, this);

    return Expression.newBuilder()
        .setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setOutputType(toProto(expr.getType()))
                .setFunctionReference(extensionCollector.getFunctionReference(expr.declaration()))
                .addAllArguments(
                    expr.arguments().stream()
                        .map(a -> a.accept(expr.declaration(), 0, argVisitor, context))
                        .collect(java.util.stream.Collectors.toList()))
                .addAllOptions(
                    expr.options().stream()
                        .map(ExpressionProtoConverter::from)
                        .collect(java.util.stream.Collectors.toList())))
        .build();
  }

  public static FunctionOption from(io.substrait.expression.FunctionOption option) {
    return FunctionOption.newBuilder()
        .setName(option.getName())
        .addAllPreference(option.values())
        .build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.Cast expr, EmptyVisitationContext context) {
    return Expression.newBuilder()
        .setCast(
            Expression.Cast.newBuilder()
                .setInput(toProto(expr.input()))
                .setType(toProto(expr.getType()))
                .setFailureBehavior(expr.failureBehavior().toProto()))
        .build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.SingleOrList expr, EmptyVisitationContext context)
      throws RuntimeException {
    return Expression.newBuilder()
        .setSingularOrList(
            Expression.SingularOrList.newBuilder()
                .setValue(toProto(expr.condition()))
                .addAllOptions(toProto(expr.options())))
        .build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.MultiOrList expr, EmptyVisitationContext context)
      throws RuntimeException {
    return Expression.newBuilder()
        .setMultiOrList(
            Expression.MultiOrList.newBuilder()
                .addAllValue(toProto(expr.conditions()))
                .addAllOptions(
                    expr.optionCombinations().stream()
                        .map(
                            r ->
                                Expression.MultiOrList.Record.newBuilder()
                                    .addAllFields(toProto(r.values()))
                                    .build())
                        .collect(java.util.stream.Collectors.toList())))
        .build();
  }

  private Expression nested(Consumer<Expression.Nested.Builder> consumer) {
    Expression.Nested.Builder builder = Expression.Nested.newBuilder();
    builder.setNullable(builder.getNullable());
    consumer.accept(builder);
    return Expression.newBuilder().setNested(builder).build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.NestedList expr, EmptyVisitationContext context)
      throws RuntimeException {
    return nested(
        bldr -> {
          List<Expression> values =
              expr.values().stream().map(this::toProto).collect(Collectors.toList());
          bldr.setNullable(expr.nullable())
              .setList(Expression.Nested.List.newBuilder().addAllValues(values));
        });
  }

  @Override
  public Expression visit(FieldReference expr, EmptyVisitationContext context) {

    Expression.ReferenceSegment seg = null;
    for (FieldReference.ReferenceSegment segment : expr.segments()) {
      Expression.ReferenceSegment.Builder protoSegment;
      if (segment instanceof FieldReference.StructField) {
        FieldReference.StructField f = (FieldReference.StructField) segment;
        Expression.ReferenceSegment.StructField.Builder bldr =
            Expression.ReferenceSegment.StructField.newBuilder().setField(f.offset());
        if (seg != null) {
          bldr.setChild(seg);
        }
        protoSegment = Expression.ReferenceSegment.newBuilder().setStructField(bldr);
      } else if (segment instanceof FieldReference.ListElement) {
        FieldReference.ListElement f = (FieldReference.ListElement) segment;
        Expression.ReferenceSegment.ListElement.Builder bldr =
            Expression.ReferenceSegment.ListElement.newBuilder().setOffset(f.offset());
        if (seg != null) {
          bldr.setChild(seg);
        }
        protoSegment = Expression.ReferenceSegment.newBuilder().setListElement(bldr);
      } else if (segment instanceof FieldReference.MapKey) {
        FieldReference.MapKey f = (FieldReference.MapKey) segment;
        Expression.ReferenceSegment.MapKey.Builder bldr =
            Expression.ReferenceSegment.MapKey.newBuilder().setMapKey(toLiteral(f.key()));
        if (seg != null) {
          bldr.setChild(seg);
        }
        protoSegment = Expression.ReferenceSegment.newBuilder().setMapKey(bldr);
      } else {
        throw new IllegalArgumentException("Unhandled type: " + segment);
      }
      seg = protoSegment.build();
    }

    Expression.FieldReference.Builder out =
        Expression.FieldReference.newBuilder().setDirectReference(seg);

    if (expr.inputExpression().isPresent()) {
      out.setExpression(toProto(expr.inputExpression().get()));
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
  public Expression visit(
      io.substrait.expression.Expression.SetPredicate expr, EmptyVisitationContext context)
      throws RuntimeException {
    return Expression.newBuilder()
        .setSubquery(
            Expression.Subquery.newBuilder()
                .setSetPredicate(
                    Expression.Subquery.SetPredicate.newBuilder()
                        .setPredicateOp(expr.predicateOp().toProto())
                        .setTuples(toProto(expr.tuples()))
                        .build())
                .build())
        .build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.ScalarSubquery expr, EmptyVisitationContext context)
      throws RuntimeException {
    return Expression.newBuilder()
        .setSubquery(
            Expression.Subquery.newBuilder()
                .setScalar(
                    Expression.Subquery.Scalar.newBuilder().setInput(toProto(expr.input())).build())
                .build())
        .build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.InPredicate expr, EmptyVisitationContext context)
      throws RuntimeException {
    return Expression.newBuilder()
        .setSubquery(
            Expression.Subquery.newBuilder()
                .setInPredicate(
                    Expression.Subquery.InPredicate.newBuilder()
                        .setHaystack(toProto(expr.haystack()))
                        .addAllNeedles(toProto(expr.needles()))
                        .build())
                .build())
        .build();
  }

  @Override
  public Expression visit(
      io.substrait.expression.Expression.WindowFunctionInvocation expr,
      EmptyVisitationContext context)
      throws RuntimeException {
    FunctionArg.FuncArgVisitor<FunctionArgument, EmptyVisitationContext, RuntimeException>
        argVisitor = FunctionArg.toProto(typeProtoConverter, this);
    List<FunctionArgument> args =
        expr.arguments().stream()
            .map(a -> a.accept(expr.declaration(), 0, argVisitor, context))
            .collect(java.util.stream.Collectors.toList());
    Type outputType = toProto(expr.getType());

    List<Expression> partitionExprs = toProto(expr.partitionBy());

    List<SortField> sortFields =
        expr.sort().stream()
            .map(
                s ->
                    SortField.newBuilder()
                        .setDirection(s.direction().toProto())
                        .setExpr(toProto(s.expr()))
                        .build())
            .collect(java.util.stream.Collectors.toList());

    Expression.WindowFunction.Bound lowerBound = BoundConverter.convert(expr.lowerBound());
    Expression.WindowFunction.Bound upperBound = BoundConverter.convert(expr.upperBound());

    return Expression.newBuilder()
        .setWindowFunction(
            Expression.WindowFunction.newBuilder()
                .setFunctionReference(extensionCollector.getFunctionReference(expr.declaration()))
                .addAllArguments(args)
                .setOutputType(outputType)
                .setPhase(expr.aggregationPhase().toProto())
                .setInvocation(expr.invocation().toProto())
                .addAllSorts(sortFields)
                .addAllPartitions(partitionExprs)
                .setBoundsType(expr.boundsType().toProto())
                .setLowerBound(lowerBound)
                .setUpperBound(upperBound)
                .addAllOptions(
                    expr.options().stream()
                        .map(ExpressionProtoConverter::from)
                        .collect(java.util.stream.Collectors.toList())))
        .build();
  }

  public static class BoundConverter
      implements WindowBound.WindowBoundVisitor<Expression.WindowFunction.Bound, RuntimeException> {
    private static final BoundConverter TO_BOUND_VISITOR = new BoundConverter();

    public static Expression.WindowFunction.Bound convert(WindowBound bound) {
      return bound.accept(TO_BOUND_VISITOR);
    }

    private BoundConverter() {}

    @Override
    public Expression.WindowFunction.Bound visit(WindowBound.Preceding preceding) {
      return Expression.WindowFunction.Bound.newBuilder()
          .setPreceding(
              Expression.WindowFunction.Bound.Preceding.newBuilder().setOffset(preceding.offset()))
          .build();
    }

    @Override
    public Expression.WindowFunction.Bound visit(WindowBound.Following following) {
      return Expression.WindowFunction.Bound.newBuilder()
          .setFollowing(
              Expression.WindowFunction.Bound.Following.newBuilder().setOffset(following.offset()))
          .build();
    }

    @Override
    public Expression.WindowFunction.Bound visit(WindowBound.CurrentRow currentRow) {
      return Expression.WindowFunction.Bound.newBuilder()
          .setCurrentRow(Expression.WindowFunction.Bound.CurrentRow.getDefaultInstance())
          .build();
    }

    @Override
    public Expression.WindowFunction.Bound visit(WindowBound.Unbounded unbounded) {
      return Expression.WindowFunction.Bound.newBuilder()
          .setUnbounded(Expression.WindowFunction.Bound.Unbounded.getDefaultInstance())
          .build();
    }
  }
}
