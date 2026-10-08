package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.Expression.Literal;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlItemOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Calcite {@link RexCall} ITEM operators into Substrait {@link FieldReference}
 * expressions.
 *
 * <p>Handles dereferencing of ROW, ARRAY, and MAP types using literal indices or keys.
 */
public class FieldSelectionConverter implements CallConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(FieldSelectionConverter.class);

  private final TypeConverter typeConverter;

  /**
   * Creates a converter for field selection operations.
   *
   * @param typeConverter converter for Substrait ↔ Calcite type mappings
   */
  public FieldSelectionConverter(TypeConverter typeConverter) {
    super();
    this.typeConverter = typeConverter;
  }

  /**
   * Converts a Calcite ITEM operator into a Substrait {@link FieldReference}, if applicable.
   *
   * <p>Supports:
   *
   * <ul>
   *   <li>ROW dereference by integer index
   *   <li>ARRAY dereference by integer index, preserving the safe operator's indexing base
   *   <li>MAP dereference by string key
   * </ul>
   *
   * @param call the Calcite ITEM operator call
   * @param topLevelConverter function to convert nested operands
   * @return an {@link Optional} containing the converted expression, or empty if not applicable
   */
  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    if (!(call.getKind() == SqlKind.ITEM)) {
      return Optional.empty();
    }

    RexNode toDereference = call.getOperands().get(0);
    RexNode reference = call.getOperands().get(1);

    if (reference.getKind() != SqlKind.LITERAL || !(reference instanceof RexLiteral)) {
      LOGGER
          .atWarn()
          .log(
              "Found item operator without literal kind/type. This isn't handled well. Reference"
                  + " was {} with toString {}.",
              reference.getKind().name(),
              reference);
      return Optional.empty();
    }

    SqlTypeName containerType = toDereference.getType().getSqlTypeName();
    // A Substrait list reference returns null for an out-of-range index, so it cannot preserve
    // the error behavior of OFFSET or ORDINAL. Decline before the array operand is converted.
    if (containerType == SqlTypeName.ARRAY && !isSafeArrayOperator(call.getOperator())) {
      return Optional.empty();
    }

    Expression input = topLevelConverter.apply(toDereference);

    switch (containerType) {
      case ROW:
        {
          Literal literal = (new LiteralConverter(typeConverter)).convert((RexLiteral) reference);
          Optional<Integer> index = toInt(literal);
          if (index.isEmpty()) {
            return Optional.empty();
          }
          if (input instanceof FieldReference) {
            return Optional.of(((FieldReference) input).dereferenceStruct(index.get()));
          } else {
            return Optional.of(FieldReference.newStructReference(index.get(), input));
          }
        }
      case ARRAY:
        {
          SqlItemOperator operator = (SqlItemOperator) call.getOperator();
          long offset = Integer.MAX_VALUE;
          if (!((RexLiteral) reference).isNull()) {
            Literal literal = (new LiteralConverter(typeConverter)).convert((RexLiteral) reference);
            Optional<Long> index = toLong(literal);
            if (index.isEmpty()) {
              return Optional.empty();
            }
            // Negative Substrait offsets count from the end, whereas Calcite returns null below
            // the operator's base. Substrait lists have at most Integer.MAX_VALUE elements, so that
            // zero-based offset cannot select an element. Keep the reference to evaluate the
            // array operand and preserve its element type, including for a null index.
            if (index.get() >= operator.offset) {
              offset = Math.min(index.get() - operator.offset, Integer.MAX_VALUE);
            }
          }

          if (input instanceof FieldReference) {
            return Optional.of(((FieldReference) input).dereferenceList((int) offset));
          } else {
            return Optional.of(FieldReference.newListReference((int) offset, input));
          }
        }

      case MAP:
        {
          Literal literal = (new LiteralConverter(typeConverter)).convert((RexLiteral) reference);
          Optional<String> mapKey = toString(literal);
          if (mapKey.isEmpty()) {
            return Optional.empty();
          }

          Expression.Literal keyLiteral = ExpressionCreator.string(false, mapKey.get());
          if (input instanceof FieldReference) {
            return Optional.of(((FieldReference) input).dereferenceMap(keyLiteral));
          } else {
            return Optional.of(FieldReference.newMapReference(keyLiteral, input));
          }
        }
    }

    return Optional.empty();
  }

  private static boolean isSafeArrayOperator(SqlOperator operator) {
    if (!(operator instanceof SqlItemOperator)) {
      return false;
    }
    SqlItemOperator itemOperator = (SqlItemOperator) operator;
    return itemOperator.safe && (itemOperator.offset == 0 || itemOperator.offset == 1);
  }

  /**
   * Converts a numeric literal to an integer index.
   *
   * @param l literal to convert
   * @return optional integer value, empty if not numeric
   */
  private Optional<Integer> toInt(Expression.Literal l) {
    if (l instanceof Expression.I8Literal) {
      return Optional.of(((Expression.I8Literal) l).value());
    } else if (l instanceof Expression.I16Literal) {
      return Optional.of(((Expression.I16Literal) l).value());
    } else if (l instanceof Expression.I32Literal) {
      return Optional.of(((Expression.I32Literal) l).value());
    } else if (l instanceof Expression.I64Literal) {
      return Optional.of((int) ((Expression.I64Literal) l).value());
    }
    LOGGER.atWarn().log("Literal expected to be int type but was not. {}.", l);
    return Optional.empty();
  }

  private Optional<Long> toLong(Expression.Literal literal) {
    if (literal instanceof Expression.I64Literal) {
      return Optional.of(((Expression.I64Literal) literal).value());
    }
    return toInt(literal).map(Integer::longValue);
  }

  /**
   * Converts a fixed-char literal to a string key.
   *
   * @param l literal to convert
   * @return optional string value, empty if not a fixed-char literal
   */
  public Optional<String> toString(Expression.Literal l) {
    if (!(l instanceof Expression.FixedCharLiteral)) {
      LOGGER.atWarn().log("Literal expected to be char type but was not. {}", l);
      return Optional.empty();
    }

    return Optional.of(((Expression.FixedCharLiteral) l).value());
  }
}
