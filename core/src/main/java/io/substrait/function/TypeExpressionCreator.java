package io.substrait.function;

import io.substrait.type.TypeCreator;
import java.util.Arrays;

public class TypeExpressionCreator extends TypeCreator
    implements ExtendedTypeCreator<TypeExpression, TypeExpression> {

  public static final TypeExpressionCreator REQUIRED = new TypeExpressionCreator(false);
  public static final TypeExpressionCreator NULLABLE = new TypeExpressionCreator(true);

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

  public TypeExpression intervalDayE(TypeExpression precision) {
    return TypeExpression.IntervalDay.builder().nullable(nullable).precision(precision).build();
  }

  public TypeExpression intervalCompoundE(TypeExpression precision) {
    return TypeExpression.IntervalCompound.builder()
        .nullable(nullable)
        .precision(precision)
        .build();
  }

  public TypeExpression precisionTimestampE(TypeExpression precision) {
    return TypeExpression.PrecisionTimestamp.builder()
        .nullable(nullable)
        .precision(precision)
        .build();
  }

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

  public static class Assign {
    String name;
    TypeExpression expr;

    public Assign() {}

    public Assign(final String name, final TypeExpression expr) {
      this.name = name;
      this.expr = expr;
    }

    public String name() {
      return name;
    }

    public TypeExpression expr() {
      return expr;
    }
  }
  ;

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

  public static TypeExpression plus(TypeExpression left, TypeExpression right) {
    return binary(TypeExpression.BinaryOperation.OpType.ADD, left, right);
  }

  public static TypeExpression minus(TypeExpression left, TypeExpression right) {
    return binary(TypeExpression.BinaryOperation.OpType.SUBTRACT, left, right);
  }

  public static TypeExpression binary(
      TypeExpression.BinaryOperation.OpType op, TypeExpression left, TypeExpression right) {
    return TypeExpression.BinaryOperation.builder().opType(op).left(left).right(right).build();
  }

  public static TypeExpression.IntegerLiteral i(int i) {
    return TypeExpression.IntegerLiteral.builder().value(i).build();
  }
}
