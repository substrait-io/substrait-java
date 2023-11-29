package io.substrait.extendedexpression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.*;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ExtendedExpressionProtoConverterTest extends TestBase {
  static final String NAMESPACE = "/functions_arithmetic_decimal.yaml";

  @Test
  public void toProtoTest() {
    // create predefined POJO extended expression
    Expression.ScalarFunctionInvocation scalarFunctionInvocation =
        b.scalarFn(
            NAMESPACE,
            "add:dec_dec",
            TypeCreator.REQUIRED.BOOLEAN,
            ImmutableFieldReference.builder()
                .addSegments(FieldReference.StructField.of(0))
                .type(TypeCreator.REQUIRED.decimal(10, 2))
                .build(),
            ExpressionCreator.i32(false, 183));

    ImmutableExpressionReference expressionReference =
        ImmutableExpressionReference.builder()
            .expressionType(
                ImmutableExpressionType.builder().expression(scalarFunctionInvocation).build())
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
            .referredExpressions(expressionReferences)
            .baseSchema(namedStruct);

    // convert POJO extended expression into PROTOBUF extended expression
    io.substrait.proto.ExtendedExpression proto =
        new ExtendedExpressionProtoConverter().toProto(extendedExpression.build());

    assertEquals(NAMESPACE, proto.getExtensionUrisList().get(0).getUri());
    assertEquals("add:dec_dec", proto.getExtensionsList().get(0).getExtensionFunction().getName());
  }
}
