package io.substrait.extended.expression;

import com.google.protobuf.util.JsonFormat;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.*;

public class MyCliente {
  public static void main(String[] args) throws IOException {
    SimpleExtension.ExtensionCollection defaultExtensionCollection = SimpleExtension.loadDefaults();
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
    List<Type> dataTypes =
        Arrays.asList(
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING,
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING);
    NamedStruct namedStruct =
        NamedStruct.of(
            columnNames, Type.Struct.builder().fields(dataTypes).nullable(false).build());

    ImmutableNamedStruct.builder()
        .addNames("id")
        .struct(Type.Struct.builder().nullable(false).addFields(TypeCreator.REQUIRED.I32).build())
        .build();

    ImmutableExtendedExpression.Builder builder =
        ImmutableExtendedExpression.builder()
            .putAllReferredExpr(indexToExpressionMap)
            .baseSchema(namedStruct);

    System.out.println(
        "JsonFormat.printer().print(getFilterExtendedExpression): "
            + JsonFormat.printer()
                .print(new ExtendedExpressionProtoConverter().toProto(builder.build())));
  }
}
