package io.substrait.extended.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.*;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.NamedStruct;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.ProtoTypeConverter;
import java.io.IOException;
import java.util.*;

/**
 * Converts from {@link io.substrait.proto.ExtendedExpression} to {@link
 * io.substrait.extended.expression.ExtendedExpression}
 */
public class ProtoExtendedExpressionConverter {
  private final SimpleExtension.ExtensionCollection extensionCollection;

  public ProtoExtendedExpressionConverter() throws IOException {
    this(SimpleExtension.loadDefaults());
  }

  public ProtoExtendedExpressionConverter(SimpleExtension.ExtensionCollection extensionCollection) {
    this.extensionCollection = extensionCollection;
  }

  private final ProtoTypeConverter protoTypeConverter =
      new ProtoTypeConverter(
          new ExtensionCollector(), ImmutableSimpleExtension.ExtensionCollection.builder().build());

  public ExtendedExpression from(io.substrait.proto.ExtendedExpression extendedExpression) {
    // fill in simple extension information through a discovery in the current proto-extended
    // expression
    ExtensionLookup functionLookup =
        ImmutableExtensionLookup.builder()
            .from(extendedExpression.getExtensionUrisList(), extendedExpression.getExtensionsList())
            .build();

    NamedStruct baseSchemaProto = extendedExpression.getBaseSchema();
    io.substrait.type.NamedStruct namedStruct = convertNamedStrutProtoToPojo(baseSchemaProto);

    ProtoExpressionConverter protoExpressionConverter =
        new ProtoExpressionConverter(
            functionLookup, this.extensionCollection, namedStruct.struct(), null);

    List<ExtendedExpression.ExpressionReference> expressionReferences = new ArrayList<>();
    for (ExpressionReference expressionReference : extendedExpression.getReferredExprList()) {
      Expression expressionPojo =
          protoExpressionConverter.from(expressionReference.getExpression());
      expressionReferences.add(
          ImmutableExpressionReference.builder()
              .referredExpr(expressionPojo)
              .addAllOutputNames(expressionReference.getOutputNamesList())
              .build());
    }

    ImmutableExtendedExpression.Builder builder =
        ImmutableExtendedExpression.builder()
            .referredExpr(expressionReferences)
            .advancedExtension(
                Optional.ofNullable(
                    extendedExpression.hasAdvancedExtensions()
                        ? extendedExpression.getAdvancedExtensions()
                        : null))
            .baseSchema(namedStruct);
    return builder.build();
  }

  private io.substrait.type.NamedStruct convertNamedStrutProtoToPojo(NamedStruct namedStruct) {
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
