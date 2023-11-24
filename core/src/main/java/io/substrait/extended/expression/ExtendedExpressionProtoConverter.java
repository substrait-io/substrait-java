package io.substrait.extended.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.type.proto.TypeProtoConverter;

/**
 * Converts from {@link io.substrait.extended.expression.ExtendedExpression} to {@link
 * ExtendedExpression}
 */
public class ExtendedExpressionProtoConverter {
  public ExtendedExpression toProto(
      io.substrait.extended.expression.ExtendedExpression extendedExpression) {

    ExtendedExpression.Builder extendedExpressionBuilder = ExtendedExpression.newBuilder();
    ExtensionCollector functionCollector = new ExtensionCollector();

    final ExpressionProtoConverter expressionProtoConverter =
        new ExpressionProtoConverter(functionCollector, null);

    for (io.substrait.extended.expression.ExtendedExpression.ExpressionReference
        expressionReference : extendedExpression.getReferredExpr()) {

      io.substrait.proto.Expression expressionProto =
          expressionProtoConverter.visit(
              (Expression.ScalarFunctionInvocation) expressionReference.getReferredExpr());

      ExpressionReference.Builder expressionReferenceBuilder =
          ExpressionReference.newBuilder()
              .setExpression(expressionProto)
              .addAllOutputNames(expressionReference.getOutputNames());

      extendedExpressionBuilder.addReferredExpr(expressionReferenceBuilder);
    }
    extendedExpressionBuilder.setBaseSchema(
        extendedExpression.getBaseSchema().toProto(new TypeProtoConverter(functionCollector)));

    // the process of adding simple extensions, such as extensionURIs and extensions, is handled on
    // the fly
    functionCollector.addExtensionsToExtendedExpression(extendedExpressionBuilder);
    if (extendedExpression.getAdvancedExtension().isPresent()) {
      extendedExpressionBuilder.setAdvancedExtensions(
          extendedExpression.getAdvancedExtension().get());
    }
    return extendedExpressionBuilder.build();
  }
}
