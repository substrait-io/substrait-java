package io.substrait.extendedexpression;

import io.substrait.expression.Expression;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.ImmutableExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.NamedStruct;
import io.substrait.relation.ProtoAggregateFunctionConverter;
import io.substrait.type.proto.ProtoTypeConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Converts from {@link io.substrait.proto.ExtendedExpression} to {@link ExtendedExpression} */
public class ProtoExtendedExpressionConverter {
  private final SimpleExtension.ExtensionCollection extensionCollection;

  private final ProtoTypeConverter protoTypeConverter =
      new ProtoTypeConverter(
          new ExtensionCollector(), SimpleExtension.ExtensionCollection.builder().build());

  public ProtoExtendedExpressionConverter() {
    this(SimpleExtension.loadDefaults());
  }

  public ProtoExtendedExpressionConverter(SimpleExtension.ExtensionCollection extensionCollection) {
    this.extensionCollection = extensionCollection;
  }

  public ExtendedExpression from(io.substrait.proto.ExtendedExpression extendedExpression) {
    // fill in simple extension information through a discovery in the current proto-extended
    // expression
    ExtensionLookup functionLookup =
        ImmutableExtensionLookup.builder().from(extendedExpression).build();

    NamedStruct baseSchemaProto = extendedExpression.getBaseSchema();

    io.substrait.type.NamedStruct namedStruct =
        io.substrait.type.NamedStruct.fromProto(baseSchemaProto, protoTypeConverter);

    ProtoExpressionConverter protoExpressionConverter =
        new ProtoExpressionConverter(
            functionLookup, this.extensionCollection, namedStruct.struct(), null);

    List<ExtendedExpression.ExpressionReferenceBase> expressionReferences = new ArrayList<>();

    for (ExpressionReference expressionReference : extendedExpression.getReferredExprList()) {

      switch (expressionReference.getExprTypeCase()) {
        case EXPRESSION:
          Expression expressionPojo =
              protoExpressionConverter.from(expressionReference.getExpression());
          ImmutableExpressionReference buildExpression =
              ImmutableExpressionReference.builder()
                  .expression(expressionPojo)
                  .addAllOutputNames(expressionReference.getOutputNamesList())
                  .build();
          expressionReferences.add(buildExpression);
          break;
        case MEASURE:
          io.substrait.relation.Aggregate.Measure measure =
              io.substrait.relation.Aggregate.Measure.builder()
                  .function(
                      new ProtoAggregateFunctionConverter(
                              functionLookup, extensionCollection, protoExpressionConverter)
                          .from(expressionReference.getMeasure()))
                  .build();
          ImmutableAggregateFunctionReference buildMeasure =
              ImmutableAggregateFunctionReference.builder()
                  .measure(measure)
                  .addAllOutputNames(expressionReference.getOutputNamesList())
                  .build();
          expressionReferences.add(buildMeasure);
          break;
        case EXPRTYPE_NOT_SET:
          throw new UnsupportedOperationException(
              "You must specify the expression type in conversion from proto to pojo Extended Expressions: Expression or Aggregate Function.");
        default:
          throw new UnsupportedOperationException(
              "Only Expression or Aggregate Function type are supported in conversion from proto to pojo Extended Expressions.");
      }
    }

    ImmutableExtendedExpression.Builder builder =
        ImmutableExtendedExpression.builder()
            .referredExpressions(expressionReferences)
            .advancedExtension(
                Optional.ofNullable(
                    extendedExpression.hasAdvancedExtensions()
                        ? extendedExpression.getAdvancedExtensions()
                        : null))
            .baseSchema(namedStruct);

    return builder.build();
  }
}
