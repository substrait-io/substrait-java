package io.substrait.expression.proto;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FieldReference.ReferenceSegment;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionOption;
import io.substrait.expression.WindowBound;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.ConsistentPartitionWindowRel;
import io.substrait.proto.Expression.FieldReference.ReferenceTypeCase;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.SortField;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeVisitor;
import io.substrait.type.proto.ProtoTypeConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Converts from {@link io.substrait.proto.Expression} to {@link io.substrait.expression.Expression}
 */
public class ProtoExpressionConverter {
  public static final Type.Struct EMPTY_TYPE = Type.Struct.builder().nullable(false).build();

  private final ExtensionLookup lookup;
  private final SimpleExtension.ExtensionCollection extensions;
  private final Type.Struct rootType;
  private final ProtoTypeConverter protoTypeConverter;
  private final ProtoRelConverter protoRelConverter;

  public ProtoExpressionConverter(
      ExtensionLookup lookup,
      SimpleExtension.ExtensionCollection extensions,
      Type.Struct rootType,
      ProtoRelConverter relConverter) {
    this.lookup = lookup;
    this.extensions = extensions;
    this.rootType = Objects.requireNonNull(rootType, "rootType");
    this.protoTypeConverter = new ProtoTypeConverter(lookup, extensions);
    this.protoRelConverter = relConverter;
  }

  public FieldReference from(io.substrait.proto.Expression.FieldReference reference) {
    io.substrait.proto.Expression.FieldReference.ReferenceTypeCase refTypeCase =
        reference.getReferenceTypeCase();

    if (refTypeCase == ReferenceTypeCase.MASKED_REFERENCE) {
      throw new IllegalArgumentException("Unsupported type: " + refTypeCase);
    }

    if (refTypeCase != ReferenceTypeCase.DIRECT_REFERENCE) {
      throw new IllegalArgumentException("Unhandled type: " + refTypeCase);
    }

    switch (reference.getRootTypeCase()) {
      case EXPRESSION:
        return FieldReference.ofExpression(
            from(reference.getExpression()),
            getDirectReferenceSegments(reference.getDirectReference()));
      case ROOT_REFERENCE:
        return FieldReference.ofRoot(
            rootType, getDirectReferenceSegments(reference.getDirectReference()));
      case OUTER_REFERENCE:
        return FieldReference.newRootStructOuterReference(
            reference.getDirectReference().getStructField().getField(),
            rootType,
            reference.getOuterReference().getStepsOut());
      case ROOTTYPE_NOT_SET:
      default:
        throw new IllegalArgumentException("Unhandled type: " + reference.getRootTypeCase());
    }
  }

  private List<ReferenceSegment> getDirectReferenceSegments(
      io.substrait.proto.Expression.ReferenceSegment segment) {
    List<ReferenceSegment> results = new ArrayList<>();

    while (segment != io.substrait.proto.Expression.ReferenceSegment.getDefaultInstance()) {
      final ReferenceSegment mappedSegment;
      switch (segment.getReferenceTypeCase()) {
        case MAP_KEY:
          io.substrait.proto.Expression.ReferenceSegment.MapKey mapKey = segment.getMapKey();
          segment = mapKey.getChild();
          mappedSegment = FieldReference.MapKey.of(from(mapKey.getMapKey()));
          break;
        case STRUCT_FIELD:
          io.substrait.proto.Expression.ReferenceSegment.StructField structField =
              segment.getStructField();
          segment = structField.getChild();
          mappedSegment = FieldReference.StructField.of(structField.getField());
          break;
        case LIST_ELEMENT:
          io.substrait.proto.Expression.ReferenceSegment.ListElement listElement =
              segment.getListElement();
          segment = listElement.getChild();
          mappedSegment = FieldReference.ListElement.of(listElement.getOffset());
          break;
        case REFERENCETYPE_NOT_SET:
        default:
          throw new IllegalArgumentException("Unhandled type: " + segment.getReferenceTypeCase());
      }

      results.add(mappedSegment);
    }

    Collections.reverse(results);

    return results;
  }

  public Expression from(io.substrait.proto.Expression expr) {
    switch (expr.getRexTypeCase()) {
      case LITERAL:
        return from(expr.getLiteral());
      case SELECTION:
        return from(expr.getSelection());
      case SCALAR_FUNCTION:
        {
          io.substrait.proto.Expression.ScalarFunction scalarFunction = expr.getScalarFunction();
          int functionReference = scalarFunction.getFunctionReference();
          SimpleExtension.ScalarFunctionVariant declaration =
              lookup.getScalarFunction(functionReference, extensions);
          FunctionArg.ProtoFrom pF = new FunctionArg.ProtoFrom(this, protoTypeConverter);
          List<FunctionArg> args =
              IntStream.range(0, scalarFunction.getArgumentsCount())
                  .mapToObj(i -> pF.convert(declaration, i, scalarFunction.getArguments(i)))
                  .collect(Collectors.toList());
          List<FunctionOption> options =
              scalarFunction.getOptionsList().stream()
                  .map(ProtoExpressionConverter::fromFunctionOption)
                  .collect(Collectors.toList());
          return Expression.ScalarFunctionInvocation.builder()
              .addAllArguments(args)
              .declaration(declaration)
              .outputType(protoTypeConverter.from(scalarFunction.getOutputType()))
              .options(options)
              .build();
        }
      case WINDOW_FUNCTION:
        return fromWindowFunction(expr.getWindowFunction());
      case IF_THEN:
        {
          io.substrait.proto.Expression.IfThen ifThen = expr.getIfThen();
          List<Expression.IfClause> clauses =
              ifThen.getIfsList().stream()
                  .map(t -> ExpressionCreator.ifThenClause(from(t.getIf()), from(t.getThen())))
                  .collect(Collectors.toList());
          return ExpressionCreator.ifThenStatement(from(ifThen.getElse()), clauses);
        }
      case SWITCH_EXPRESSION:
        {
          io.substrait.proto.Expression.SwitchExpression switchExpr = expr.getSwitchExpression();
          List<Expression.SwitchClause> clauses =
              switchExpr.getIfsList().stream()
                  .map(t -> ExpressionCreator.switchClause(from(t.getIf()), from(t.getThen())))
                  .collect(Collectors.toList());
          return ExpressionCreator.switchStatement(
              from(switchExpr.getMatch()), from(switchExpr.getElse()), clauses);
        }
      case SINGULAR_OR_LIST:
        {
          io.substrait.proto.Expression.SingularOrList orList = expr.getSingularOrList();
          List<Expression> values =
              orList.getOptionsList().stream().map(this::from).collect(Collectors.toList());
          return Expression.SingleOrList.builder()
              .condition(from(orList.getValue()))
              .addAllOptions(values)
              .build();
        }
      case MULTI_OR_LIST:
        {
          io.substrait.proto.Expression.MultiOrList multiOrList = expr.getMultiOrList();
          List<Expression.MultiOrListRecord> values =
              multiOrList.getOptionsList().stream()
                  .map(
                      t ->
                          Expression.MultiOrListRecord.builder()
                              .addAllValues(
                                  t.getFieldsList().stream()
                                      .map(this::from)
                                      .collect(Collectors.toList()))
                              .build())
                  .collect(Collectors.toList());
          return Expression.MultiOrList.builder()
              .addAllOptionCombinations(values)
              .addAllConditions(
                  multiOrList.getValueList().stream().map(this::from).collect(Collectors.toList()))
              .build();
        }
      case CAST:
        return ExpressionCreator.cast(
            protoTypeConverter.from(expr.getCast().getType()),
            from(expr.getCast().getInput()),
            Expression.FailureBehavior.fromProto(expr.getCast().getFailureBehavior()));
      case SUBQUERY:
        {
          switch (expr.getSubquery().getSubqueryTypeCase()) {
            case SET_PREDICATE:
              {
                io.substrait.relation.Rel rel =
                    protoRelConverter.from(expr.getSubquery().getSetPredicate().getTuples());
                return Expression.SetPredicate.builder()
                    .tuples(rel)
                    .predicateOp(
                        Expression.PredicateOp.fromProto(
                            expr.getSubquery().getSetPredicate().getPredicateOp()))
                    .build();
              }
            case SCALAR:
              {
                io.substrait.relation.Rel rel =
                    protoRelConverter.from(expr.getSubquery().getScalar().getInput());
                return Expression.ScalarSubquery.builder()
                    .input(rel)
                    .type(
                        rel.getRecordType()
                            .accept(
                                new TypeVisitor.TypeThrowsVisitor<Type, RuntimeException>(
                                    "Expected struct field") {
                                  @Override
                                  public Type visit(Type.Struct type) throws RuntimeException {
                                    if (type.fields().size() != 1) {
                                      throw new UnsupportedOperationException(
                                          "Scalar subquery must have exactly one field");
                                    }
                                    // Result can be null if the query returns no rows
                                    return type.fields().get(0);
                                  }
                                }))
                    .build();
              }
            case IN_PREDICATE:
              {
                io.substrait.relation.Rel rel =
                    protoRelConverter.from(expr.getSubquery().getInPredicate().getHaystack());
                List<Expression> needles =
                    expr.getSubquery().getInPredicate().getNeedlesList().stream()
                        .map(e -> this.from(e))
                        .collect(Collectors.toList());
                return Expression.InPredicate.builder().haystack(rel).needles(needles).build();
              }
            case SET_COMPARISON:
              throw new UnsupportedOperationException(
                  "Unsupported subquery type: " + expr.getSubquery().getSubqueryTypeCase());
            default:
              throw new IllegalArgumentException(
                  "Unknown subquery type: " + expr.getSubquery().getSubqueryTypeCase());
          }
        }

      // TODO enum.
      case ENUM:
        throw new UnsupportedOperationException("Unsupported type: " + expr.getRexTypeCase());
      default:
        throw new IllegalArgumentException("Unknown type: " + expr.getRexTypeCase());
    }
  }

  public Expression.WindowFunctionInvocation fromWindowFunction(
      io.substrait.proto.Expression.WindowFunction windowFunction) {
    int functionReference = windowFunction.getFunctionReference();
    SimpleExtension.WindowFunctionVariant declaration =
        lookup.getWindowFunction(functionReference, extensions);
    FunctionArg.ProtoFrom argVisitor = new FunctionArg.ProtoFrom(this, protoTypeConverter);

    List<FunctionArg> args =
        fromFunctionArgumentList(
            windowFunction.getArgumentsCount(),
            argVisitor,
            declaration,
            windowFunction::getArguments);
    List<Expression> partitionExprs =
        windowFunction.getPartitionsList().stream().map(this::from).collect(Collectors.toList());
    List<Expression.SortField> sortFields =
        windowFunction.getSortsList().stream()
            .map(this::fromSortField)
            .collect(Collectors.toList());
    List<FunctionOption> options =
        windowFunction.getOptionsList().stream()
            .map(ProtoExpressionConverter::fromFunctionOption)
            .collect(Collectors.toList());

    WindowBound lowerBound = toWindowBound(windowFunction.getLowerBound());
    WindowBound upperBound = toWindowBound(windowFunction.getUpperBound());

    return Expression.WindowFunctionInvocation.builder()
        .arguments(args)
        .declaration(declaration)
        .outputType(protoTypeConverter.from(windowFunction.getOutputType()))
        .aggregationPhase(Expression.AggregationPhase.fromProto(windowFunction.getPhase()))
        .partitionBy(partitionExprs)
        .sort(sortFields)
        .boundsType(Expression.WindowBoundsType.fromProto(windowFunction.getBoundsType()))
        .lowerBound(lowerBound)
        .upperBound(upperBound)
        .invocation(Expression.AggregationInvocation.fromProto(windowFunction.getInvocation()))
        .options(options)
        .build();
  }

  public ConsistentPartitionWindow.WindowRelFunctionInvocation fromWindowRelFunction(
      ConsistentPartitionWindowRel.WindowRelFunction windowRelFunction) {
    int functionReference = windowRelFunction.getFunctionReference();
    SimpleExtension.WindowFunctionVariant declaration =
        lookup.getWindowFunction(functionReference, extensions);
    FunctionArg.ProtoFrom argVisitor = new FunctionArg.ProtoFrom(this, protoTypeConverter);

    List<FunctionArg> args =
        fromFunctionArgumentList(
            windowRelFunction.getArgumentsCount(),
            argVisitor,
            declaration,
            windowRelFunction::getArguments);
    List<FunctionOption> options =
        windowRelFunction.getOptionsList().stream()
            .map(ProtoExpressionConverter::fromFunctionOption)
            .collect(Collectors.toList());

    WindowBound lowerBound = toWindowBound(windowRelFunction.getLowerBound());
    WindowBound upperBound = toWindowBound(windowRelFunction.getUpperBound());

    return ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
        .arguments(args)
        .declaration(declaration)
        .outputType(protoTypeConverter.from(windowRelFunction.getOutputType()))
        .aggregationPhase(Expression.AggregationPhase.fromProto(windowRelFunction.getPhase()))
        .boundsType(Expression.WindowBoundsType.fromProto(windowRelFunction.getBoundsType()))
        .lowerBound(lowerBound)
        .upperBound(upperBound)
        .invocation(Expression.AggregationInvocation.fromProto(windowRelFunction.getInvocation()))
        .options(options)
        .build();
  }

  private WindowBound toWindowBound(io.substrait.proto.Expression.WindowFunction.Bound bound) {
    switch (bound.getKindCase()) {
      case PRECEDING:
        return WindowBound.Preceding.of(bound.getPreceding().getOffset());
      case FOLLOWING:
        return WindowBound.Following.of(bound.getFollowing().getOffset());
      case CURRENT_ROW:
        return WindowBound.CURRENT_ROW;
      case UNBOUNDED:
        return WindowBound.UNBOUNDED;
      case KIND_NOT_SET:
        // per the spec, the lower and upper bounds default to the start or end of the partition
        // respectively if not set
        return WindowBound.UNBOUNDED;
      default:
        throw new IllegalArgumentException("Unsupported bound kind: " + bound.getKindCase());
    }
  }

  public Expression.Literal from(io.substrait.proto.Expression.Literal literal) {
    switch (literal.getLiteralTypeCase()) {
      case BOOLEAN:
        return ExpressionCreator.bool(literal.getNullable(), literal.getBoolean());
      case I8:
        return ExpressionCreator.i8(literal.getNullable(), literal.getI8());
      case I16:
        return ExpressionCreator.i16(literal.getNullable(), literal.getI16());
      case I32:
        return ExpressionCreator.i32(literal.getNullable(), literal.getI32());
      case I64:
        return ExpressionCreator.i64(literal.getNullable(), literal.getI64());
      case FP32:
        return ExpressionCreator.fp32(literal.getNullable(), literal.getFp32());
      case FP64:
        return ExpressionCreator.fp64(literal.getNullable(), literal.getFp64());
      case STRING:
        return ExpressionCreator.string(literal.getNullable(), literal.getString());
      case BINARY:
        return ExpressionCreator.binary(literal.getNullable(), literal.getBinary());
      case TIMESTAMP:
        return ExpressionCreator.timestamp(literal.getNullable(), literal.getTimestamp());
      case TIMESTAMP_TZ:
        return ExpressionCreator.timestampTZ(literal.getNullable(), literal.getTimestampTz());
      case PRECISION_TIMESTAMP:
        return ExpressionCreator.precisionTimestamp(
            literal.getNullable(),
            literal.getPrecisionTimestamp().getValue(),
            literal.getPrecisionTimestamp().getPrecision());
      case PRECISION_TIMESTAMP_TZ:
        return ExpressionCreator.precisionTimestampTZ(
            literal.getNullable(),
            literal.getPrecisionTimestampTz().getValue(),
            literal.getPrecisionTimestampTz().getPrecision());
      case DATE:
        return ExpressionCreator.date(literal.getNullable(), literal.getDate());
      case TIME:
        return ExpressionCreator.time(literal.getNullable(), literal.getTime());
      case INTERVAL_YEAR_TO_MONTH:
        return ExpressionCreator.intervalYear(
            literal.getNullable(),
            literal.getIntervalYearToMonth().getYears(),
            literal.getIntervalYearToMonth().getMonths());
      case INTERVAL_DAY_TO_SECOND:
        {
          // Handle deprecated version that doesn't provide precision and that uses microseconds
          // instead of subseconds, for backwards compatibility
          int precision =
              literal.getIntervalDayToSecond().hasPrecision()
                  ? literal.getIntervalDayToSecond().getPrecision()
                  : 6; // microseconds
          long subseconds =
              literal.getIntervalDayToSecond().hasPrecision()
                  ? literal.getIntervalDayToSecond().getSubseconds()
                  : literal.getIntervalDayToSecond().getMicroseconds();
          return ExpressionCreator.intervalDay(
              literal.getNullable(),
              literal.getIntervalDayToSecond().getDays(),
              literal.getIntervalDayToSecond().getSeconds(),
              subseconds,
              precision);
        }
      case INTERVAL_COMPOUND:
        {
          if (!literal.getIntervalCompound().getIntervalDayToSecond().hasPrecision()) {
            throw new UnsupportedOperationException(
                "Interval compound with deprecated version of interval day (ie. no precision) is not supported");
          }
          return ExpressionCreator.intervalCompound(
              literal.getNullable(),
              literal.getIntervalCompound().getIntervalYearToMonth().getYears(),
              literal.getIntervalCompound().getIntervalYearToMonth().getMonths(),
              literal.getIntervalCompound().getIntervalDayToSecond().getDays(),
              literal.getIntervalCompound().getIntervalDayToSecond().getSeconds(),
              literal.getIntervalCompound().getIntervalDayToSecond().getSubseconds(),
              literal.getIntervalCompound().getIntervalDayToSecond().getPrecision());
        }
      case FIXED_CHAR:
        return ExpressionCreator.fixedChar(literal.getNullable(), literal.getFixedChar());
      case VAR_CHAR:
        return ExpressionCreator.varChar(
            literal.getNullable(),
            literal.getVarChar().getValue(),
            literal.getVarChar().getLength());
      case FIXED_BINARY:
        return ExpressionCreator.fixedBinary(literal.getNullable(), literal.getFixedBinary());
      case DECIMAL:
        return ExpressionCreator.decimal(
            literal.getNullable(),
            literal.getDecimal().getValue(),
            literal.getDecimal().getPrecision(),
            literal.getDecimal().getScale());
      case STRUCT:
        return ExpressionCreator.struct(
            literal.getNullable(),
            literal.getStruct().getFieldsList().stream()
                .map(this::from)
                .collect(Collectors.toList()));
      case MAP:
        return ExpressionCreator.map(
            literal.getNullable(),
            literal.getMap().getKeyValuesList().stream()
                .collect(Collectors.toMap(kv -> from(kv.getKey()), kv -> from(kv.getValue()))));
      case EMPTY_MAP:
        {
          // literal.getNullable() is intentionally ignored in favor of the nullability
          // specified in the literal.getEmptyMap() type.
          Type.Map mapType = protoTypeConverter.fromMap(literal.getEmptyMap());
          return ExpressionCreator.emptyMap(mapType.nullable(), mapType.key(), mapType.value());
        }
      case UUID:
        return ExpressionCreator.uuid(literal.getNullable(), literal.getUuid());
      case NULL:
        return ExpressionCreator.typedNull(protoTypeConverter.from(literal.getNull()));
      case LIST:
        return ExpressionCreator.list(
            literal.getNullable(),
            literal.getList().getValuesList().stream()
                .map(this::from)
                .collect(Collectors.toList()));
      case EMPTY_LIST:
        {
          // literal.getNullable() is intentionally ignored in favor of the nullability
          // specified in the literal.getEmptyList() type.
          Type.ListType listType = protoTypeConverter.fromList(literal.getEmptyList());
          return ExpressionCreator.emptyList(listType.nullable(), listType.elementType());
        }
      case USER_DEFINED:
        {
          io.substrait.proto.Expression.Literal.UserDefined userDefinedLiteral =
              literal.getUserDefined();

          SimpleExtension.Type type =
              lookup.getType(userDefinedLiteral.getTypeReference(), extensions);
          String urn = type.urn();
          String name = type.name();
          List<io.substrait.type.Type.Parameter> typeParameters =
              userDefinedLiteral.getTypeParametersList().stream()
                  .map(protoTypeConverter::from)
                  .collect(Collectors.toList());

          switch (userDefinedLiteral.getValCase()) {
            case VALUE:
              return ExpressionCreator.userDefinedLiteralAny(
                  literal.getNullable(), urn, name, typeParameters, userDefinedLiteral.getValue());
            case STRUCT:
              return ExpressionCreator.userDefinedLiteralStruct(
                  literal.getNullable(),
                  urn,
                  name,
                  typeParameters,
                  userDefinedLiteral.getStruct().getFieldsList().stream()
                      .map(this::from)
                      .collect(Collectors.toList()));
            case VAL_NOT_SET:
              throw new IllegalStateException(
                  "UserDefined literal has no value (neither 'value' nor 'struct' is set)");
            default:
              throw new IllegalStateException(
                  "Unknown UserDefined literal value case: " + userDefinedLiteral.getValCase());
          }
        }
      default:
        throw new IllegalStateException("Unexpected value: " + literal.getLiteralTypeCase());
    }
  }

  public Expression.StructLiteral from(io.substrait.proto.Expression.Literal.Struct struct) {
    return Expression.StructLiteral.builder()
        .fields(struct.getFieldsList().stream().map(this::from).collect(Collectors.toList()))
        .build();
  }

  public Expression.NestedStruct from(io.substrait.proto.Expression.Nested.Struct struct) {
    return Expression.NestedStruct.builder()
        .fields(struct.getFieldsList().stream().map(this::from).collect(Collectors.toList()))
        .build();
  }

  private static List<FunctionArg> fromFunctionArgumentList(
      int argumentsCount,
      FunctionArg.ProtoFrom argVisitor,
      SimpleExtension.Function declaration,
      Function<Integer, FunctionArgument> argFunction) {
    return IntStream.range(0, argumentsCount)
        .mapToObj(i -> argVisitor.convert(declaration, i, argFunction.apply(i)))
        .collect(Collectors.toList());
  }

  public Expression.SortField fromSortField(SortField s) {
    return Expression.SortField.builder()
        .direction(Expression.SortDirection.fromProto(s.getDirection()))
        .expr(from(s.getExpr()))
        .build();
  }

  public static FunctionOption fromFunctionOption(io.substrait.proto.FunctionOption o) {
    return FunctionOption.builder().name(o.getName()).addAllValues(o.getPreferenceList()).build();
  }
}
