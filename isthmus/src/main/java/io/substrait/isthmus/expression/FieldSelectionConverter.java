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
   *   <li>ARRAY dereference by integer index
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
              "Found item operator without literal kind/type. This isn't handled well. Reference was {} with toString {}.",
              reference.getKind().name(),
              reference);
      return Optional.empty();
    }

    Literal literal = (new LiteralConverter(typeConverter)).convert((RexLiteral) reference);

    Expression input = topLevelConverter.apply(toDereference);

    switch (toDereference.getType().getSqlTypeName()) {
      case ROW:
        {
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
          Optional<Integer> index = toInt(literal);
          if (index.isEmpty()) {
            return Optional.empty();
          }

          if (input instanceof FieldReference) {
            return Optional.of(((FieldReference) input).dereferenceList(index.get()));
          } else {
            return Optional.of(FieldReference.newListReference(index.get(), input));
          }
        }

      case MAP:
        {
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
