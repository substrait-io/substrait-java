package io.substrait.extendedexpression;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtoExtendedExpressionConverterTest extends TestBase {
  static final String NAMESPACE = "/functions_arithmetic_decimal.yaml";

  @Test
  public void fromTest() throws IOException {
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

    List<io.substrait.extendedexpression.ExtendedExpression.ExpressionReference>
        expressionReferences = new ArrayList<>();
    expressionReferences.add(expressionReference);

    ImmutableNamedStruct namedStruct =
        ImmutableNamedStruct.builder()
            .addNames("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT")
            .struct(
                Type.Struct.builder()
                    .nullable(false)
                    .addFields(
                        TypeCreator.REQUIRED.decimal(10, 2),
                        TypeCreator.REQUIRED.STRING,
                        TypeCreator.REQUIRED.decimal(10, 2),
                        TypeCreator.REQUIRED.STRING)
                    .build())
            .build();

    // pojo initial extended expression
    ImmutableExtendedExpression extendedExpressionPojoInitial =
        ImmutableExtendedExpression.builder()
            .referredExpressions(expressionReferences)
            .baseSchema(namedStruct)
            .build();

    // proto extended expression
    ExtendedExpression extendedExpressionProto =
        new ExtendedExpressionProtoConverter().toProto(extendedExpressionPojoInitial);

    // pojo final extended expression
    io.substrait.extendedexpression.ExtendedExpression extendedExpressionPojoFinal =
        new ProtoExtendedExpressionConverter().from(extendedExpressionProto);

    // validate extended expression pojo initial equals to final roundtrip
    Assertions.assertEquals(extendedExpressionPojoInitial, extendedExpressionPojoFinal);
  }
}
