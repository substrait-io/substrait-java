package io.substrait.extended.expression;

import com.google.protobuf.util.JsonFormat;
import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtoExtendedExpressionConverterTest extends TestBase {
  @Test
  public void fromTest() throws IOException {
    Optional<Expression.ScalarFunctionInvocation> equal =
        defaultExtensionCollection.scalarFunctions().stream()
            .filter(
                s -> {
                  System.out.println(":>>>>");
                  System.out.println(s);
                  System.out.println(s.uri());
                  System.out.println(s.returnType());
                  System.out.println(s.description());
                  System.out.println("s.name(): " + s.name());
                  System.out.println(s.key());
                  return s.name().equalsIgnoreCase("add");
                })
            .findFirst()
            .map(
                declaration -> {
                  System.out.println("declaration: " + declaration);
                  System.out.println("declaration.name(): " + declaration.name());
                  return ExpressionCreator.scalarFunction(
                      declaration,
                      TypeCreator.REQUIRED.BOOLEAN,
                      ImmutableFieldReference.builder()
                          .addSegments(FieldReference.StructField.of(0))
                          .type(TypeCreator.REQUIRED.I32)
                          .build(),
                      ExpressionCreator.i32(false, 183));
                });

    Map<Integer, Expression> indexToExpressionMap = new HashMap<>();
    indexToExpressionMap.put(0, equal.get());
    List<String> columnNames = Arrays.asList("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT");
    /*
    List<Type> dataTypes =
        Arrays.asList(
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING,
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING);
    NamedStruct namedStruct =
        NamedStruct.of(
            columnNames, Type.Struct.builder().fields(dataTypes).nullable(false).build());
     */
      ImmutableNamedStruct id = ImmutableNamedStruct.builder()
              .addNames("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT")
              .struct(Type.Struct.builder().nullable(false).addFields(TypeCreator.NULLABLE.I32,
                      TypeCreator.REQUIRED.STRING,
                      TypeCreator.REQUIRED.I32,
                      TypeCreator.REQUIRED.STRING).build())
              .build();

      ImmutableExtendedExpression.Builder builder =
        ImmutableExtendedExpression.builder()
            .putAllReferredExpr(indexToExpressionMap)
            .baseSchema(id);

    ExtendedExpression proto = new ExtendedExpressionProtoConverter().toProto(builder.build());

    System.out.println("=======POJO 01=======");
    System.out.println("xxxx: " + builder);
    System.out.println("=======PROTO 02=======");
    System.out.println("yyyy: " + JsonFormat.printer().print(proto));

    System.out.println("=======POJO 03=======");
    io.substrait.extended.expression.ExtendedExpression from =
        new ProtoExtendedExpressionConverter().from(proto);
    System.out.println("zzzz: " + from);
      System.out.println("11111111");


      Assertions.assertEquals(from, builder.build());
  }
}
