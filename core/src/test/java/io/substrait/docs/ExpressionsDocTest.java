package io.substrait.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.TestBase;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.NamedScan;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/core/expressions.md}. Regions marked with {@code // --8<--
 * [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--} snippet includes.
 */
class ExpressionsDocTest extends TestBase {

  @Test
  void literalsWithExpressionCreator() {
    // --8<-- [start:literals]
    Expression.I32Literal a = ExpressionCreator.i32(false, 76); // non-nullable 76
    Expression.StrLiteral s = ExpressionCreator.string(true, "hello"); // nullable "hello"
    Expression.BoolLiteral b = ExpressionCreator.bool(false, true);
    Expression.FP64Literal f = ExpressionCreator.fp64(false, 2.5);
    Expression.DateLiteral d = ExpressionCreator.date(false, 19_000); // days since epoch
    // --8<-- [end:literals]
    assertNotNull(a);
    assertNotNull(s);
    assertNotNull(b);
    assertNotNull(f);
    assertNotNull(d);
  }

  @Test
  void typedNull() {
    // --8<-- [start:typed-null]
    Expression.NullLiteral n = ExpressionCreator.typedNull(TypeCreator.NULLABLE.I32);
    // --8<-- [end:typed-null]
    assertNotNull(n);
  }

  @Test
  void castsAndFunctionInvocations() {
    Expression someExpression = ExpressionCreator.i32(false, 1);
    SimpleExtension.ScalarFunctionVariant declaration =
        extensions.getScalarFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "add:i32_i32"));
    Expression argExpr1 = ExpressionCreator.i32(false, 1);
    Expression argExpr2 = ExpressionCreator.i32(false, 2);
    // --8<-- [start:creator-nonliteral]
    // cast with an explicit failure behavior
    Expression cast =
        ExpressionCreator.cast(
            TypeCreator.REQUIRED.I64, someExpression, Expression.FailureBehavior.THROW_EXCEPTION);

    // scalar function invocation from a resolved declaration
    Expression.ScalarFunctionInvocation call =
        ExpressionCreator.scalarFunction(declaration, TypeCreator.REQUIRED.I32, argExpr1, argExpr2);
    // --8<-- [end:creator-nonliteral]
    assertNotNull(cast);
    assertNotNull(call);
  }

  @Test
  void builderLiteralHelpers() {
    // --8<-- [start:builder-literals]
    SubstraitBuilder b = new SubstraitBuilder();

    b.bool(true); // BoolLiteral
    b.i8(10); // I8Literal
    b.i16(100); // I16Literal
    b.i32(1000); // I32Literal
    b.i64(10_000L); // I64Literal
    b.fp32(1.5f); // FP32Literal
    b.fp64(2.5); // FP64Literal
    b.str("foo"); // StrLiteral
    // --8<-- [end:builder-literals]
  }

  @Test
  void fieldReferences() {
    SubstraitBuilder b = new SubstraitBuilder();
    // --8<-- [start:field-references]
    NamedScan scan =
        b.namedScan(
            List.of("t"),
            List.of("a", "b"),
            List.of(TypeCreator.REQUIRED.I32, TypeCreator.REQUIRED.STRING));

    FieldReference col0 = b.fieldReference(scan, 0);
    List<FieldReference> cols = b.fieldReferences(scan, 0, 1);
    // --8<-- [end:field-references]
    assertNotNull(col0);
    assertNotNull(cols);
  }

  @Test
  void builderCast() {
    SubstraitBuilder b = new SubstraitBuilder();
    // --8<-- [start:builder-cast]
    Expression cast = b.cast(b.i32(1), TypeCreator.REQUIRED.I64);
    // --8<-- [end:builder-cast]
    assertNotNull(cast);
  }

  @Test
  void arithmeticComparisonBoolean() {
    SubstraitBuilder b = new SubstraitBuilder();
    Expression cond1 = b.equal(b.i32(1), b.i32(2));
    Expression cond2 = b.equal(b.i32(3), b.i32(4));
    Expression cond = cond1;
    Expression expr = b.i32(0);
    // --8<-- [start:arithmetic-boolean]
    Expression left = b.i32(10);
    Expression right = b.i32(20);

    b.add(left, right); // add:i32_i32     -> functions_arithmetic
    b.subtract(left, right);
    b.multiply(left, right);
    b.divide(left, right);
    b.negate(left);

    b.equal(left, right); // equal:any_any   -> functions_comparison, returns boolean
    b.and(cond1, cond2); // and:bool        -> functions_boolean
    b.or(cond1, cond2);
    b.not(cond);
    b.isNull(expr); // is_null:any     -> functions_comparison
    // --8<-- [end:arithmetic-boolean]
  }

  @Test
  void genericScalarFunction() {
    SubstraitBuilder b = new SubstraitBuilder();
    Expression strArg = b.str("hello");
    Expression startArg = b.i32(1);
    Expression lengthArg = b.i32(3);
    // --8<-- [start:generic-scalarfn]
    Expression.ScalarFunctionInvocation substr =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_STRING,
            "substring:str_i32_i32",
            TypeCreator.REQUIRED.STRING,
            strArg,
            startArg,
            lengthArg);
    // --8<-- [end:generic-scalarfn]
    assertNotNull(substr);
  }
}
