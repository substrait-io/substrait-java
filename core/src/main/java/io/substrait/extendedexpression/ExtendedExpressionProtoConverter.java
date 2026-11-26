package io.substrait.extendedexpression;

import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.NestingDetacher;
import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Version;
import io.substrait.relation.AggregateFunctionProtoConverter;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.proto.TypeProtoConverter;
import io.substrait.util.EmptyVisitationContext;
import java.util.HashSet;

/**
 * Converts from {@link io.substrait.extendedexpression.ExtendedExpression} to {@link
 * io.substrait.proto.ExtendedExpression}
 */
public class ExtendedExpressionProtoConverter {
  private final NestingDetacher nestingDetacher;

  /**
   * Creates a converter that uses {@link NestingDetacher#defaultDetacher()}, which leaves an
   * extended expression unchanged unless its relations or expressions nest too deeply for protobuf
   * parsers with default limits.
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

    RelProtoConverter relConverter = new RelProtoConverter(functionCollector);
    final ExpressionProtoConverter expressionProtoConverter =
        relConverter.getExpressionProtoConverter();
    final TypeProtoConverter typeProtoConverter = relConverter.getTypeProtoConverter();
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
    if (!extendedExpression.getRetainedExtensions().isEmpty()
        || !extendedExpression.getRetainedExtensionUrns().isEmpty()) {
      // Raw subtrees may use anchors that the referred expressions never visit.
      if (!new HashSet<>(extendedExpression.getRetainedExtensionUrns())
              .containsAll(builder.getExtensionUrnsList())
          || !new HashSet<>(extendedExpression.getRetainedExtensions())
              .containsAll(builder.getExtensionsList())) {
        throw new UnsupportedOperationException(
            "Cannot renumber extension anchors used by opaque detached subtrees");
      }
      builder
          .clearExtensionUrns()
          .addAllExtensionUrns(extendedExpression.getRetainedExtensionUrns());
      builder.clearExtensions().addAllExtensions(extendedExpression.getRetainedExtensions());
    }
    builder.addAllExpectedTypeUrls(extendedExpression.getExpectedTypeUrls());
    if (extendedExpression.getAdvancedExtension().isPresent()) {
      builder.setAdvancedExtensions(extendedExpression.getAdvancedExtension().get());
    }
    builder.setVersion(extendedExpression.getVersion());
    // Retain existing ordinals before appending any newly detached subtrees.
    builder.addAllDetachedExpressions(extendedExpression.getRetainedDetachedExpressions());
    builder.addAllDetachedRels(extendedExpression.getRetainedDetachedRelations());
    ExtendedExpression bounded = nestingDetacher.detach(builder.build());
    Version version =
        NestingDetacher.versionForTables(
            bounded.getVersion(),
            bounded.getDetachedExpressionsCount(),
            bounded.getDetachedRelsCount());
    return version == bounded.getVersion()
        ? bounded
        : bounded.toBuilder().setVersion(version).build();
  }
}
