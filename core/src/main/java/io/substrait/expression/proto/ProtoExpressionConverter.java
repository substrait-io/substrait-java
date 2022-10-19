package io.substrait.expression.proto;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionLookup;
import io.substrait.expression.ImmutableExpression;
import io.substrait.function.SimpleExtension;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.type.Type;
import io.substrait.type.proto.FromProto;
import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProtoExpressionConverter {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ProtoExpressionConverter.class);

  public static final Type.Struct EMPTY_TYPE = Type.Struct.builder().nullable(false).build();

  private final FunctionLookup lookup;
  private final SimpleExtension.ExtensionCollection extensions;
  private final Type.Struct rootType;

  public ProtoExpressionConverter(
      FunctionLookup lookup, SimpleExtension.ExtensionCollection extensions, Type.Struct rootType) {
    this.lookup = lookup;
    this.extensions = extensions;
    this.rootType = Objects.requireNonNull(rootType, "rootType");
  }

  public FieldReference from(io.substrait.proto.Expression.FieldReference reference) {
    switch (reference.getReferenceTypeCase()) {
      case DIRECT_REFERENCE:
        {
          io.substrait.proto.Expression.ReferenceSegment segment = reference.getDirectReference();

          var segments = new ArrayList<FieldReference.ReferenceSegment>();
          while (segment != io.substrait.proto.Expression.ReferenceSegment.getDefaultInstance()) {
            FieldReference.ReferenceSegment seg = null;
            switch (segment.getReferenceTypeCase()) {
              case MAP_KEY:
                {
                  var mapKey = segment.getMapKey();
                  segment = mapKey.getChild();
                  seg = FieldReference.MapKey.of(from(mapKey.getMapKey()));
                  break;
                }
              case STRUCT_FIELD:
                {
                  var structField = segment.getStructField();
                  segment = structField.getChild();
                  seg = FieldReference.StructField.of(structField.getField());
                  break;
                }
              case LIST_ELEMENT:
                {
                  var listElement = segment.getListElement();
                  segment = listElement.getChild();
                  seg = FieldReference.ListElement.of(listElement.getOffset());
                  break;
                }
              case REFERENCETYPE_NOT_SET:
                throw new IllegalArgumentException(
                    "Unhandled type: " + segment.getReferenceTypeCase());
            }
            segments.add(seg);
          }

          FieldReference fieldReference = null;
          switch (reference.getRootTypeCase()) {
            case EXPRESSION:
              fieldReference =
                  FieldReference.ofExpression(from(reference.getExpression()), segments);
              break;
            case ROOT_REFERENCE:
              fieldReference = FieldReference.ofRoot(rootType, segments);
              break;
            case OUTER_REFERENCE:
              fieldReference =
                  FieldReference.newRootStructOuterReference(
                      reference.getDirectReference().getStructField().getField(),
                      rootType,
                      reference.getOuterReference().getStepsOut());
              break;
            case ROOTTYPE_NOT_SET:
              throw new IllegalArgumentException("Unhandled type: " + reference.getRootTypeCase());
          }

          return fieldReference;
        }
      case MASKED_REFERENCE:
        throw new IllegalArgumentException("Unsupported type: " + reference.getReferenceTypeCase());
      default:
        throw new IllegalArgumentException("Unhandled type: " + reference.getReferenceTypeCase());
    }
  }

  public Expression from(io.substrait.proto.Expression expr) {
    switch (expr.getRexTypeCase()) {
      case LITERAL:
        return from(expr.getLiteral());
      case SELECTION:
        return from(expr.getSelection());
      case SCALAR_FUNCTION:
        {
          var scalarFunction = expr.getScalarFunction();
          var functionReference = scalarFunction.getFunctionReference();
          var declaration = lookup.getScalarFunction(functionReference, extensions);
          var pF = new FunctionArg.ProtoFrom(this);
          var args =
              IntStream.range(0, scalarFunction.getArgumentsCount())
                  .mapToObj(i -> pF.convert(declaration, i, scalarFunction.getArguments(i)))
                  .collect(java.util.stream.Collectors.toList());
          return ImmutableExpression.ScalarFunctionInvocation.builder()
              .addAllArguments(args)
              .declaration(declaration)
              .outputType(from(expr.getScalarFunction().getOutputType()))
              .build();
        }
      case IF_THEN:
        {
          var ifThen = expr.getIfThen();
          var clauses =
              ifThen.getIfsList().stream()
                  .map(t -> ExpressionCreator.ifThenClause(from(t.getIf()), from(t.getThen())))
                  .collect(java.util.stream.Collectors.toList());
          return ExpressionCreator.ifThenStatement(from(ifThen.getElse()), clauses);
        }
      case SWITCH_EXPRESSION:
        {
          var switchExpr = expr.getSwitchExpression();
          var clauses =
              switchExpr.getIfsList().stream()
                  .map(t -> ExpressionCreator.switchClause(from(t.getIf()), from(t.getThen())))
                  .collect(java.util.stream.Collectors.toList());
          return ExpressionCreator.switchStatement(from(switchExpr.getElse()), clauses);
        }
      case SINGULAR_OR_LIST:
        {
          var orList = expr.getSingularOrList();
          var values =
              orList.getOptionsList().stream()
                  .map(this::from)
                  .collect(java.util.stream.Collectors.toList());
          return ImmutableExpression.SingleOrList.builder()
              .condition(from(orList.getValue()))
              .addAllOptions(values)
              .build();
        }
      case MULTI_OR_LIST:
        {
          var multiOrList = expr.getMultiOrList();
          var values =
              multiOrList.getOptionsList().stream()
                  .map(
                      t ->
                          ImmutableExpression.MultiOrListRecord.builder()
                              .addAllValues(
                                  t.getFieldsList().stream()
                                      .map(this::from)
                                      .collect(java.util.stream.Collectors.toList()))
                              .build())
                  .collect(java.util.stream.Collectors.toList());
          return ImmutableExpression.MultiOrList.builder()
              .addAllOptionCombinations(values)
              .addAllConditions(
                  multiOrList.getValueList().stream()
                      .map(this::from)
                      .collect(java.util.stream.Collectors.toList()))
              .build();
        }
      case CAST:
        return ExpressionCreator.cast(
            FromProto.from(expr.getCast().getType()), from(expr.getCast().getInput()));
      case SUBQUERY:
        {
          switch (expr.getSubquery().getSubqueryTypeCase()) {
            case SET_PREDICATE:
              {
                var rel =
                    new ProtoRelConverter(lookup, extensions)
                        .from(expr.getSubquery().getSetPredicate().getTuples());
                return ImmutableExpression.SetPredicate.builder()
                    .tuples(rel)
                    .predicateOp(
                        Expression.PredicateOp.fromProto(
                            expr.getSubquery().getSetPredicate().getPredicateOp()))
                    .build();
              }
            case SCALAR:
              {
                var rel =
                    new ProtoRelConverter(lookup, extensions)
                        .from(expr.getSubquery().getScalar().getInput());
                return ImmutableExpression.ScalarSubquery.builder()
                    .input(rel)
                    .type(rel.getRecordType())
                    .build();
              }
            case IN_PREDICATE:
              {
                var rel =
                    new ProtoRelConverter(lookup, extensions)
                        .from(expr.getSubquery().getInPredicate().getHaystack());
                var needles =
                    expr.getSubquery().getInPredicate().getNeedlesList().stream()
                        .map(this::from)
                        .collect(java.util.stream.Collectors.toList());
                return ImmutableExpression.InPredicate.builder()
                    .haystack(rel)
                    .needles(needles)
                    .type(rel.getRecordType())
                    .build();
              }
            case SET_COMPARISON:
              {
                throw new UnsupportedOperationException(
                    "Unsupported subquery type: " + expr.getSubquery().getSubqueryTypeCase());
              }
            default:
              {
                throw new IllegalArgumentException(
                    "Unknown subquery type: " + expr.getSubquery().getSubqueryTypeCase());
              }
          }
        }

        // TODO window, enum.
      case WINDOW_FUNCTION:
      case ENUM:
        throw new UnsupportedOperationException("Unsupported type: " + expr.getRexTypeCase());
      default:
        throw new IllegalArgumentException("Unknown type: " + expr.getRexTypeCase());
    }
  }

  private static Type from(io.substrait.proto.Type type) {
    return FromProto.from(type);
  }

  public static Expression.Literal from(io.substrait.proto.Expression.Literal literal) {
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
        return ExpressionCreator.intervalDay(
            literal.getNullable(),
            literal.getIntervalDayToSecond().getDays(),
            literal.getIntervalDayToSecond().getSeconds());
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
                .map(ProtoExpressionConverter::from)
                .collect(java.util.stream.Collectors.toList()));
      case MAP:
        return ExpressionCreator.map(
            literal.getNullable(),
            literal.getMap().getKeyValuesList().stream()
                .collect(Collectors.toMap(kv -> from(kv.getKey()), kv -> from(kv.getValue()))));
      case TIMESTAMP_TZ:
        return ExpressionCreator.timestampTZ(literal.getNullable(), literal.getTimestampTz());
      case UUID:
        return ExpressionCreator.uuid(literal.getNullable(), literal.getUuid());
      case NULL:
        return ExpressionCreator.typedNull(from(literal.getNull()));
      case LIST:
        return ExpressionCreator.list(
            literal.getNullable(),
            literal.getList().getValuesList().stream()
                .map(ProtoExpressionConverter::from)
                .collect(java.util.stream.Collectors.toList()));
      default:
        throw new IllegalStateException("Unexpected value: " + literal.getLiteralTypeCase());
    }
  }
}
