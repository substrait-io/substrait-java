package io.substrait.expression.proto;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ExtensionLookup;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableExpression;
import io.substrait.function.SimpleExtension;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.type.Type;
import io.substrait.type.proto.ProtoTypeConverter;
import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Converts from {@link io.substrait.proto.Expression} to {@link io.substrait.expression.Expression}
 */
public class ProtoExpressionConverter {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ProtoExpressionConverter.class);

  public static final Type.Struct EMPTY_TYPE = Type.Struct.builder().nullable(false).build();

  private final ExtensionLookup lookup;
  private final SimpleExtension.ExtensionCollection extensions;
  private final Type.Struct rootType;
  private final ProtoTypeConverter protoTypeConverter;

  public ProtoExpressionConverter(
      ExtensionLookup lookup,
      SimpleExtension.ExtensionCollection extensions,
      Type.Struct rootType) {
    this.lookup = lookup;
    this.extensions = extensions;
    this.rootType = Objects.requireNonNull(rootType, "rootType");
    this.protoTypeConverter = new ProtoTypeConverter(lookup, extensions);
  }

  public FieldReference from(io.substrait.proto.Expression.FieldReference reference) {
    switch (reference.getReferenceTypeCase()) {
      case DIRECT_REFERENCE -> {
        io.substrait.proto.Expression.ReferenceSegment segment = reference.getDirectReference();

        var segments = new ArrayList<FieldReference.ReferenceSegment>();
        while (segment != io.substrait.proto.Expression.ReferenceSegment.getDefaultInstance()) {
          segments.add(
              switch (segment.getReferenceTypeCase()) {
                case MAP_KEY -> {
                  var mapKey = segment.getMapKey();
                  segment = mapKey.getChild();
                  yield FieldReference.MapKey.of(from(mapKey.getMapKey()));
                }
                case STRUCT_FIELD -> {
                  var structField = segment.getStructField();
                  segment = structField.getChild();
                  yield FieldReference.StructField.of(structField.getField());
                }
                case LIST_ELEMENT -> {
                  var listElement = segment.getListElement();
                  segment = listElement.getChild();
                  yield FieldReference.ListElement.of(listElement.getOffset());
                }
                case REFERENCETYPE_NOT_SET -> throw new IllegalArgumentException(
                    "Unhandled type: " + segment.getReferenceTypeCase());
              });
        }

        var fieldReference =
            switch (reference.getRootTypeCase()) {
              case EXPRESSION -> FieldReference.ofExpression(
                  from(reference.getExpression()), segments);
              case ROOT_REFERENCE -> FieldReference.ofRoot(rootType, segments);
              case OUTER_REFERENCE -> FieldReference.newRootStructOuterReference(
                  reference.getDirectReference().getStructField().getField(),
                  rootType,
                  reference.getOuterReference().getStepsOut());
              case ROOTTYPE_NOT_SET -> throw new IllegalArgumentException(
                  "Unhandled type: " + reference.getRootTypeCase());
            };

        return fieldReference;
      }
      case MASKED_REFERENCE -> throw new IllegalArgumentException(
          "Unsupported type: " + reference.getReferenceTypeCase());
      default -> throw new IllegalArgumentException(
          "Unhandled type: " + reference.getReferenceTypeCase());
    }
  }

  public Expression from(io.substrait.proto.Expression expr) {
    return switch (expr.getRexTypeCase()) {
      case LITERAL -> from(expr.getLiteral());
      case SELECTION -> from(expr.getSelection());
      case SCALAR_FUNCTION -> {
        var scalarFunction = expr.getScalarFunction();
        var functionReference = scalarFunction.getFunctionReference();
        var declaration = lookup.getScalarFunction(functionReference, extensions);
        var pF = new FunctionArg.ProtoFrom(this, protoTypeConverter);
        var args =
            IntStream.range(0, scalarFunction.getArgumentsCount())
                .mapToObj(i -> pF.convert(declaration, i, scalarFunction.getArguments(i)))
                .collect(java.util.stream.Collectors.toList());
        yield ImmutableExpression.ScalarFunctionInvocation.builder()
            .addAllArguments(args)
            .declaration(declaration)
            .outputType(protoTypeConverter.from(expr.getScalarFunction().getOutputType()))
            .build();
      }
      case IF_THEN -> {
        var ifThen = expr.getIfThen();
        var clauses =
            ifThen.getIfsList().stream()
                .map(t -> ExpressionCreator.ifThenClause(from(t.getIf()), from(t.getThen())))
                .collect(java.util.stream.Collectors.toList());
        yield ExpressionCreator.ifThenStatement(from(ifThen.getElse()), clauses);
      }
      case SWITCH_EXPRESSION -> {
        var switchExpr = expr.getSwitchExpression();
        var clauses =
            switchExpr.getIfsList().stream()
                .map(t -> ExpressionCreator.switchClause(from(t.getIf()), from(t.getThen())))
                .collect(java.util.stream.Collectors.toList());
        yield ExpressionCreator.switchStatement(from(switchExpr.getElse()), clauses);
      }
      case SINGULAR_OR_LIST -> {
        var orList = expr.getSingularOrList();
        var values =
            orList.getOptionsList().stream()
                .map(this::from)
                .collect(java.util.stream.Collectors.toList());
        yield ImmutableExpression.SingleOrList.builder()
            .condition(from(orList.getValue()))
            .addAllOptions(values)
            .build();
      }
      case MULTI_OR_LIST -> {
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
        yield ImmutableExpression.MultiOrList.builder()
            .addAllOptionCombinations(values)
            .addAllConditions(
                multiOrList.getValueList().stream()
                    .map(this::from)
                    .collect(java.util.stream.Collectors.toList()))
            .build();
      }
      case CAST -> ExpressionCreator.cast(
          protoTypeConverter.from(expr.getCast().getType()), from(expr.getCast().getInput()));
      case SUBQUERY -> {
        switch (expr.getSubquery().getSubqueryTypeCase()) {
          case SET_PREDICATE -> {
            var rel =
                new ProtoRelConverter(lookup, extensions)
                    .from(expr.getSubquery().getSetPredicate().getTuples());
            yield ImmutableExpression.SetPredicate.builder()
                .tuples(rel)
                .predicateOp(
                    Expression.PredicateOp.fromProto(
                        expr.getSubquery().getSetPredicate().getPredicateOp()))
                .build();
          }
          case SCALAR -> {
            var rel =
                new ProtoRelConverter(lookup, extensions)
                    .from(expr.getSubquery().getScalar().getInput());
            yield ImmutableExpression.ScalarSubquery.builder()
                .input(rel)
                .type(rel.getRecordType())
                .build();
          }
          case IN_PREDICATE -> {
            var rel =
                new ProtoRelConverter(lookup, extensions)
                    .from(expr.getSubquery().getInPredicate().getHaystack());
            var needles =
                expr.getSubquery().getInPredicate().getNeedlesList().stream()
                    .map(e -> this.from(e))
                    .collect(java.util.stream.Collectors.toList());
            yield ImmutableExpression.InPredicate.builder()
                .haystack(rel)
                .needles(needles)
                .type(rel.getRecordType())
                .build();
          }
          case SET_COMPARISON -> {
            throw new UnsupportedOperationException(
                "Unsupported subquery type: " + expr.getSubquery().getSubqueryTypeCase());
          }
          default -> {
            throw new IllegalArgumentException(
                "Unknown subquery type: " + expr.getSubquery().getSubqueryTypeCase());
          }
        }
      }

        // TODO window, enum.
      case WINDOW_FUNCTION, ENUM -> throw new UnsupportedOperationException(
          "Unsupported type: " + expr.getRexTypeCase());
      default -> throw new IllegalArgumentException("Unknown type: " + expr.getRexTypeCase());
    };
  }

  public Expression.Literal from(io.substrait.proto.Expression.Literal literal) {
    return switch (literal.getLiteralTypeCase()) {
      case BOOLEAN -> ExpressionCreator.bool(literal.getNullable(), literal.getBoolean());
      case I8 -> ExpressionCreator.i8(literal.getNullable(), literal.getI8());
      case I16 -> ExpressionCreator.i16(literal.getNullable(), literal.getI16());
      case I32 -> ExpressionCreator.i32(literal.getNullable(), literal.getI32());
      case I64 -> ExpressionCreator.i64(literal.getNullable(), literal.getI64());
      case FP32 -> ExpressionCreator.fp32(literal.getNullable(), literal.getFp32());
      case FP64 -> ExpressionCreator.fp64(literal.getNullable(), literal.getFp64());
      case STRING -> ExpressionCreator.string(literal.getNullable(), literal.getString());
      case BINARY -> ExpressionCreator.binary(literal.getNullable(), literal.getBinary());
      case TIMESTAMP -> ExpressionCreator.timestamp(literal.getNullable(), literal.getTimestamp());
      case DATE -> ExpressionCreator.date(literal.getNullable(), literal.getDate());
      case TIME -> ExpressionCreator.time(literal.getNullable(), literal.getTime());
      case INTERVAL_YEAR_TO_MONTH -> ExpressionCreator.intervalYear(
          literal.getNullable(),
          literal.getIntervalYearToMonth().getYears(),
          literal.getIntervalYearToMonth().getMonths());
      case INTERVAL_DAY_TO_SECOND -> ExpressionCreator.intervalDay(
          literal.getNullable(),
          literal.getIntervalDayToSecond().getDays(),
          literal.getIntervalDayToSecond().getSeconds());
      case FIXED_CHAR -> ExpressionCreator.fixedChar(literal.getNullable(), literal.getFixedChar());
      case VAR_CHAR -> ExpressionCreator.varChar(
          literal.getNullable(), literal.getVarChar().getValue(), literal.getVarChar().getLength());
      case FIXED_BINARY -> ExpressionCreator.fixedBinary(
          literal.getNullable(), literal.getFixedBinary());
      case DECIMAL -> ExpressionCreator.decimal(
          literal.getNullable(),
          literal.getDecimal().getValue(),
          literal.getDecimal().getPrecision(),
          literal.getDecimal().getScale());
      case STRUCT -> ExpressionCreator.struct(
          literal.getNullable(),
          literal.getStruct().getFieldsList().stream()
              .map(this::from)
              .collect(java.util.stream.Collectors.toList()));
      case MAP -> ExpressionCreator.map(
          literal.getNullable(),
          literal.getMap().getKeyValuesList().stream()
              .collect(Collectors.toMap(kv -> from(kv.getKey()), kv -> from(kv.getValue()))));
      case TIMESTAMP_TZ -> ExpressionCreator.timestampTZ(
          literal.getNullable(), literal.getTimestampTz());
      case UUID -> ExpressionCreator.uuid(literal.getNullable(), literal.getUuid());
      case NULL -> ExpressionCreator.typedNull(protoTypeConverter.from(literal.getNull()));
      case LIST -> ExpressionCreator.list(
          literal.getNullable(),
          literal.getList().getValuesList().stream()
              .map(this::from)
              .collect(java.util.stream.Collectors.toList()));
      default -> throw new IllegalStateException(
          "Unexpected value: " + literal.getLiteralTypeCase());
    };
  }
}
