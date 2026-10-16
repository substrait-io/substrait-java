package io.substrait.extendedexpression;

import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.NestingDetacher;
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
  private final NestingDetacher nestingDetacher;

  /**
   * Creates a converter that uses {@link NestingDetacher#defaultDetacher()}, which leaves an
   * extended expression unchanged unless its expressions nest too deeply for protobuf parsers with
   * default limits.
   */
  public ExtendedExpressionProtoConverter() {
    this(NestingDetacher.defaultDetacher());
  }

  /**
   * Creates a converter that bounds the nesting depth of its output with the given detacher.
   *
   * @param nestingDetacher bounds the nesting depth of each {@link ExtendedExpression}, must not be
   *     null
   */
  public ExtendedExpressionProtoConverter(NestingDetacher nestingDetacher) {
    if (nestingDetacher == null) {
      throw new IllegalArgumentException("NestingDetacher is required");
    }
    this.nestingDetacher = nestingDetacher;
  }

  /**
   * Converts an {@link io.substrait.extendedexpression.ExtendedExpression} into its protobuf
   * representation.
   *
   * @param extendedExpression the extended expression to convert
   * @return the protobuf {@link ExtendedExpression} representation
   */
  public ExtendedExpression toProto(
      io.substrait.extendedexpression.ExtendedExpression extendedExpression) {

    ExtendedExpression.Builder builder = ExtendedExpression.newBuilder();
    ExtensionCollector functionCollector = new ExtensionCollector();

    final ExpressionProtoConverter expressionProtoConverter =
        new ExpressionProtoConverter(functionCollector, null);
    final TypeProtoConverter typeProtoConverter = new TypeProtoConverter(functionCollector);
    final AggregateFunctionProtoConverter aggregateFunctionProtoConverter =
        new AggregateFunctionProtoConverter(
            functionCollector, expressionProtoConverter, typeProtoConverter);

    for (io.substrait.extendedexpression.ExtendedExpression.ExpressionReferenceBase
        expressionReference : extendedExpression.getReferredExpressions()) {
      if (expressionReference
          instanceof io.substrait.extendedexpression.ExtendedExpression.ExpressionReference) {
        io.substrait.extendedexpression.ExtendedExpression.ExpressionReference et =
            (io.substrait.extendedexpression.ExtendedExpression.ExpressionReference)
                expressionReference;
        io.substrait.proto.Expression expressionProto =
            et.getExpression().accept(expressionProtoConverter, EmptyVisitationContext.INSTANCE);
        ExpressionReference.Builder expressionReferenceBuilder =
            ExpressionReference.newBuilder()
                .setExpression(expressionProto)
                .addAllOutputNames(expressionReference.getOutputNames());
        builder.addReferredExpr(expressionReferenceBuilder);
      } else if (expressionReference
          instanceof
          io.substrait.extendedexpression.ExtendedExpression.AggregateFunctionReference) {
        io.substrait.extendedexpression.ExtendedExpression.AggregateFunctionReference aft =
            (io.substrait.extendedexpression.ExtendedExpression.AggregateFunctionReference)
                expressionReference;
        ExpressionReference.Builder expressionReferenceBuilder =
            ExpressionReference.newBuilder()
                .setMeasure(aggregateFunctionProtoConverter.toProto(aft.getMeasure()))
                .addAllOutputNames(expressionReference.getOutputNames());
        builder.addReferredExpr(expressionReferenceBuilder);
      } else {
        throw new UnsupportedOperationException(
            "Only Expression or Aggregate Function type are supported in conversion to proto Extended Expressions");
      }
    }
    builder.setBaseSchema(extendedExpression.getBaseSchema().toProto(typeProtoConverter));

    // the process of adding simple extensions (URNs and declarations) is handled on the fly
    functionCollector.addExtensionsToExtendedExpression(builder);
    if (extendedExpression.getAdvancedExtension().isPresent()) {
      builder.setAdvancedExtensions(extendedExpression.getAdvancedExtension().get());
    }
    // Ahead of any newly detached entry, so the advanced extension's ordinals keep their targets.
    builder.addAllDetachedExpressions(extendedExpression.getRetainedDetachedExpressions());
    return nestingDetacher.detach(builder.build());
  }
}
