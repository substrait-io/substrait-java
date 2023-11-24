package io.substrait.extended.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ExtendedExpressionProtoConverterTest extends TestBase {
  @Test
  public void toProtoTest() {
    // create predefined POJO extended expression
    Optional<Expression.ScalarFunctionInvocation> scalarFunctionExpression =
        defaultExtensionCollection.scalarFunctions().stream()
            .filter(s -> s.name().equalsIgnoreCase("add"))
            .findFirst()
            .map(
                declaration ->
                    ExpressionCreator.scalarFunction(
                        declaration,
                        TypeCreator.REQUIRED.BOOLEAN,
                        ImmutableFieldReference.builder()
                            .addSegments(FieldReference.StructField.of(0))
                            .type(TypeCreator.REQUIRED.decimal(10, 2))
                            .build(),
                        ExpressionCreator.i32(false, 183)));

    ImmutableExpressionReference expressionReference =
        ImmutableExpressionReference.builder()
            .referredExpr(scalarFunctionExpression.get())
            .addOutputNames("new-column")
            .build();

    List<ExtendedExpression.ExpressionReference> expressionReferences = new ArrayList<>();
    expressionReferences.add(expressionReference);

    ImmutableNamedStruct namedStruct =
        ImmutableNamedStruct.builder()
            .addNames("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT")
            .struct(
                Type.Struct.builder()
                    .nullable(false)
                    .addFields(
                        TypeCreator.NULLABLE.decimal(10, 2),
                        TypeCreator.REQUIRED.STRING,
                        TypeCreator.REQUIRED.decimal(10, 2),
                        TypeCreator.REQUIRED.STRING)
                    .build())
            .build();

    ImmutableExtendedExpression.Builder extendedExpression =
        ImmutableExtendedExpression.builder()
            .referredExpr(expressionReferences)
            .baseSchema(namedStruct);

    // convert POJO extended expression into PROTOBUF extended expression
    io.substrait.proto.ExtendedExpression proto =
        new ExtendedExpressionProtoConverter().toProto(extendedExpression.build());

    assertEquals(
        "/functions_arithmetic_decimal.yaml", proto.getExtensionUrisList().get(0).getUri());
    assertEquals("add:dec_dec", proto.getExtensionsList().get(0).getExtensionFunction().getName());
  }
}
