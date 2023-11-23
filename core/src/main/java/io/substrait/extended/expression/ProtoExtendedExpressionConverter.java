package io.substrait.extended.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.*;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.NamedStruct;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.ProtoTypeConverter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ProtoExtendedExpressionConverter {
  private ExtensionCollector lookup = new ExtensionCollector();
  private ProtoTypeConverter protoTypeConverter =
      new ProtoTypeConverter(
          lookup, ImmutableSimpleExtension.ExtensionCollection.builder().build());

  private ProtoExpressionConverter getPprotoExpressionConverter(ExtensionLookup functionLookup) {
    return new ProtoExpressionConverter(
        functionLookup,
        this.extensionCollection,
        null,
        null);
  }

    private ProtoExpressionConverter getPprotoExpressionConverter(ExtensionLookup functionLookup, io.substrait.type.NamedStruct namedStruct) {
        return new ProtoExpressionConverter(
                functionLookup,
                this.extensionCollection,
                namedStruct.struct(),
                null);
    }

  protected final SimpleExtension.ExtensionCollection extensionCollection;

  public ProtoExtendedExpressionConverter() throws IOException {
    this(SimpleExtension.loadDefaults());
  }

  public ProtoExtendedExpressionConverter(SimpleExtension.ExtensionCollection extensionCollection) {
    this.extensionCollection = extensionCollection;
  }

  protected ProtoRelConverter getProtoRelConverter(ExtensionLookup functionLookup) {
    return new ProtoRelConverter(functionLookup, this.extensionCollection);
  }

  public ExtendedExpression from(io.substrait.proto.ExtendedExpression extendedExpressionProto) {
    ExtensionLookup functionLookup =
        ImmutableExtensionLookup.builder().from(extendedExpressionProto).build();


      // para struct
      NamedStruct baseSchema = extendedExpressionProto.getBaseSchema();
      io.substrait.type.NamedStruct namedStruct = newNamedStruct(baseSchema);

      System.out.println("namedStruct");
      System.out.println(namedStruct);

    ProtoExpressionConverter protoExpressionConverter =
        getPprotoExpressionConverter(functionLookup, namedStruct);

    Map<Integer, Expression> indexToExpressionMap = new HashMap<>();
    for (ExpressionReference expressionReference : extendedExpressionProto.getReferredExprList()) {
      System.out.println(
          "expressionReference.getExpression(): " + expressionReference.getExpression());
      indexToExpressionMap.put(
          0, protoExpressionConverter.from(expressionReference.getExpression()));
    }

    // para struct
      /*
    NamedStruct baseSchema = extendedExpressionProto.getBaseSchema();
    io.substrait.type.NamedStruct namedStruct = newNamedStruct(baseSchema);

    System.out.println("namedStruct");
    System.out.println(namedStruct);

       */

    ImmutableExtendedExpression.Builder builder =
        ImmutableExtendedExpression.builder()
            .putAllReferredExpr(indexToExpressionMap)
            .advancedExtension(
                Optional.ofNullable(
                    extendedExpressionProto.hasAdvancedExtensions()
                        ? extendedExpressionProto.getAdvancedExtensions()
                        : null))
            .baseSchema(namedStruct);
    /*
     ProtocolStringList namesList = baseSchema.getNamesList();

     Type.Struct struct = baseSchema.getStruct();
     Type types = struct.getTypes(0);
     System.out.println("types.getDescriptorForType().getName(): " + types.getDescriptorForType().);


    */

    /*
    System.out.println("namesList: " + namesList);
    System.out.println("baseSchema.getStruct(): " + baseSchema.getStruct());
    System.out.println("}}{{{{{{{{{{''------>");
    System.out.println("baseSchema.getStruct(): " + baseSchema.getStruct().getTypes(0));


     */

    /*
    ImmutableNamedStruct.builder().

    // para expression

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
                                        ExpressionCreator.i32(false, 183)
                                );
                            }
                    );

    Map<Integer, Expression> indexToExpressionMap = new HashMap<>();
    indexToExpressionMap.put(0, equal.get());

    ImmutableExtendedExpression.Builder builder =
            ImmutableExtendedExpression.builder()
                    .putAllReferredExpr(indexToExpressionMap)
                    .baseSchema(namedStruct);

             */

    return builder.build();
  }

  private io.substrait.type.NamedStruct newNamedStruct(NamedStruct namedStruct) {
    var struct = namedStruct.getStruct();
    return ImmutableNamedStruct.builder()
        .names(namedStruct.getNamesList())
        .struct(
            Type.Struct.builder()
                .fields(
                    struct.getTypesList().stream()
                        .map(protoTypeConverter::from)
                        .collect(java.util.stream.Collectors.toList()))
                .nullable(ProtoTypeConverter.isNullable(struct.getNullability()))
                .build())
        .build();
  }
}
