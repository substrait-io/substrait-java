package io.substrait.extendedexpression;

import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.relation.AggregateFunctionProtoConverter;
import io.substrait.type.proto.TypeProtoConverter;
import io.substrait.util.EmptyVisitationContext;

/**
 * Converts from {@link io.substrait.extendedexpression.ExtendedExpression} to {@link
 * io.substrait.proto.ExtendedExpression}
 */
public class ExtendedExpressionProtoConverter {

  public ExtendedExpression toProto(
      final io.substrait.extendedexpression.ExtendedExpression extendedExpression) {

    final ExtendedExpression.Builder builder = ExtendedExpression.newBuilder();
    final ExtensionCollector functionCollector = new ExtensionCollector();

    final ExpressionProtoConverter expressionProtoConverter =
        new ExpressionProtoConverter(functionCollector, null);

    for (final io.substrait.extendedexpression.ExtendedExpression.ExpressionReferenceBase
        expressionReference : extendedExpression.getReferredExpressions()) {
      if (expressionReference
          instanceof io.substrait.extendedexpression.ExtendedExpression.ExpressionReference) {
        final io.substrait.extendedexpression.ExtendedExpression.ExpressionReference et =
            (io.substrait.extendedexpression.ExtendedExpression.ExpressionReference)
                expressionReference;
        final io.substrait.proto.Expression expressionProto =
            et.getExpression().accept(expressionProtoConverter, EmptyVisitationContext.INSTANCE);
        final ExpressionReference.Builder expressionReferenceBuilder =
            ExpressionReference.newBuilder()
                .setExpression(expressionProto)
                .addAllOutputNames(expressionReference.getOutputNames());
        builder.addReferredExpr(expressionReferenceBuilder);
      } else if (expressionReference
          instanceof
          io.substrait.extendedexpression.ExtendedExpression.AggregateFunctionReference) {
        final io.substrait.extendedexpression.ExtendedExpression.AggregateFunctionReference aft =
            (io.substrait.extendedexpression.ExtendedExpression.AggregateFunctionReference)
                expressionReference;
        final ExpressionReference.Builder expressionReferenceBuilder =
            ExpressionReference.newBuilder()
                .setMeasure(
                    new AggregateFunctionProtoConverter(functionCollector)
                        .toProto(aft.getMeasure()))
                .addAllOutputNames(expressionReference.getOutputNames());
        builder.addReferredExpr(expressionReferenceBuilder);
      } else {
        throw new UnsupportedOperationException(
            "Only Expression or Aggregate Function type are supported in conversion to proto Extended Expressions");
      }
    }
    builder.setBaseSchema(
        extendedExpression.getBaseSchema().toProto(new TypeProtoConverter(functionCollector)));

    // the process of adding simple extensions, such as extensionURIs and extensions, is handled on
    // the fly
    functionCollector.addExtensionsToExtendedExpression(builder);
    if (extendedExpression.getAdvancedExtension().isPresent()) {
      builder.setAdvancedExtensions(extendedExpression.getAdvancedExtension().get());
    }
    return builder.build();
  }
}
