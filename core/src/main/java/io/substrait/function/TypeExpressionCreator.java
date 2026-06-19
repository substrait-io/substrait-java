package io.substrait.function;

import io.substrait.type.TypeCreator;
import java.util.Arrays;

/**
 * Creates {@link TypeExpression}s, including derivation expressions whose parameters are themselves
 * expressions.
 */
public class TypeExpressionCreator extends TypeCreator
    implements ExtendedTypeCreator<TypeExpression, TypeExpression> {

  /** Creator producing non-nullable type expressions. */
  public static final TypeExpressionCreator REQUIRED = new TypeExpressionCreator(false);

  /** Creator producing nullable type expressions. */
  public static final TypeExpressionCreator NULLABLE = new TypeExpressionCreator(true);

  /**
   * Creates a type-expression creator.
   *
   * @param nullable whether produced type expressions are nullable
   */
  protected TypeExpressionCreator(boolean nullable) {
    super(nullable);
  }

  @Override
  public TypeExpression fixedCharE(TypeExpression len) {
    return TypeExpression.FixedChar.builder().nullable(nullable).length(len).build();
  }

  @Override
  public TypeExpression varCharE(TypeExpression len) {
    return TypeExpression.VarChar.builder().nullable(nullable).length(len).build();
  }

  @Override
  public TypeExpression fixedBinaryE(TypeExpression len) {
    return TypeExpression.FixedBinary.builder().nullable(nullable).length(len).build();
  }

  @Override
  public TypeExpression decimalE(TypeExpression precision, TypeExpression scale) {
    return TypeExpression.Decimal.builder()
        .nullable(nullable)
        .scale(scale)
        .precision(precision)
        .build();
  }

  /**
   * Creates an interval-day type expression with a parameterized precision.
   *
   * @param precision the precision expression
   * @return the interval-day type expression
   */
  public TypeExpression intervalDayE(TypeExpression precision) {
    return TypeExpression.IntervalDay.builder().nullable(nullable).precision(precision).build();
  }

  /**
   * Creates an interval-compound type expression with a parameterized precision.
   *
   * @param precision the precision expression
   * @return the interval-compound type expression
   */
  public TypeExpression intervalCompoundE(TypeExpression precision) {
    return TypeExpression.IntervalCompound.builder()
        .nullable(nullable)
        .precision(precision)
        .build();
  }

  /**
   * Creates a precision-time type expression with a parameterized precision.
   *
   * @param precision the precision expression
   * @return the precision-time type expression
   */
  public TypeExpression precisionTimeE(TypeExpression precision) {
    return TypeExpression.PrecisionTime.builder().nullable(nullable).precision(precision).build();
  }

  /**
   * Creates a precision-timestamp type expression with a parameterized precision.
   *
   * @param precision the precision expression
   * @return the precision-timestamp type expression
   */
  public TypeExpression precisionTimestampE(TypeExpression precision) {
    return TypeExpression.PrecisionTimestamp.builder()
        .nullable(nullable)
        .precision(precision)
        .build();
  }

  /**
   * Creates a precision-timestamp-with-timezone type expression with a parameterized precision.
   *
   * @param precision the precision expression
   * @return the precision-timestamp-tz type expression
   */
  public TypeExpression precisionTimestampTZE(TypeExpression precision) {
    return TypeExpression.PrecisionTimestampTZ.builder()
        .nullable(nullable)
        .precision(precision)
        .build();
  }

  @Override
  public TypeExpression structE(TypeExpression... types) {
    return TypeExpression.Struct.builder().nullable(nullable).addFields(types).build();
  }

  @Override
  public TypeExpression structE(Iterable<? extends TypeExpression> types) {
    return TypeExpression.Struct.builder().nullable(nullable).addAllFields(types).build();
  }

  @Override
  public TypeExpression listE(TypeExpression type) {
    return TypeExpression.ListType.builder().nullable(nullable).elementType(type).build();
  }

  @Override
  public TypeExpression mapE(TypeExpression key, TypeExpression value) {
    return TypeExpression.Map.builder().nullable(nullable).key(key).value(value).build();
  }

  @Override
  public TypeExpression funcE(
      Iterable<? extends TypeExpression> parameterTypes, TypeExpression returnType) {
    return TypeExpression.Func.builder()
        .nullable(nullable)
        .addAllParameterTypes(parameterTypes)
        .returnType(returnType)
        .build();
  }

  /** A named assignment within a type derivation program. */
  public static class Assign {
    String name;
    TypeExpression expr;

    /** Creates an empty assignment. */
    public Assign() {}

    /**
     * Creates an assignment binding a name to an expression.
     *
     * @param name the assignment name
     * @param expr the assigned expression
     */
    public Assign(final String name, final TypeExpression expr) {
      this.name = name;
      this.expr = expr;
    }

    /**
     * Returns the assignment name.
     *
     * @return the name
     */
    public String name() {
      return name;
    }

    /**
     * Returns the assigned expression.
     *
     * @return the expression
     */
    public TypeExpression expr() {
      return expr;
    }
  }
  ;

  /**
   * Creates a type derivation program with the given final expression and assignments.
   *
   * @param finalExpr the final expression the program evaluates to
   * @param assignments the named assignments evaluated before the final expression
   * @return the program type expression
   */
  public static TypeExpression program(TypeExpression finalExpr, Assign... assignments) {
    return TypeExpression.ReturnProgram.builder()
        .finalExpression(finalExpr)
        .addAllAssignments(
            Arrays.stream(assignments)
                .map(
                    a ->
                        TypeExpression.ReturnProgram.Assignment.builder()
                            .name(a.name())
                            .expr(a.expr())
                            .build())
                .collect(java.util.stream.Collectors.toList()))
        .build();
  }

  /**
   * Creates a binary addition type expression.
   *
   * @param left the left operand
   * @param right the right operand
   * @return the addition expression
   */
  public static TypeExpression plus(TypeExpression left, TypeExpression right) {
    return binary(TypeExpression.BinaryOperation.OpType.ADD, left, right);
  }

  /**
   * Creates a binary subtraction type expression.
   *
   * @param left the left operand
   * @param right the right operand
   * @return the subtraction expression
   */
  public static TypeExpression minus(TypeExpression left, TypeExpression right) {
    return binary(TypeExpression.BinaryOperation.OpType.SUBTRACT, left, right);
  }

  /**
   * Creates a binary operation type expression.
   *
   * @param op the binary operator
   * @param left the left operand
   * @param right the right operand
   * @return the binary operation expression
   */
  public static TypeExpression binary(
      TypeExpression.BinaryOperation.OpType op, TypeExpression left, TypeExpression right) {
    return TypeExpression.BinaryOperation.builder().opType(op).left(left).right(right).build();
  }

  /**
   * Creates an integer literal type expression.
   *
   * @param i the integer value
   * @return the integer literal
   */
  public static TypeExpression.IntegerLiteral i(int i) {
    return TypeExpression.IntegerLiteral.builder().value(i).build();
  }
}
