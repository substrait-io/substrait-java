package io.substrait;

import static io.substrait.expression.proto.ProtoExpressionConverter.EMPTY_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.TypeCreator;
import io.substrait.util.EmptyVisitationContext;

public abstract class TestBase {

  protected static final SimpleExtension.ExtensionCollection defaultExtensionCollection =
      DefaultExtensionCatalog.DEFAULT_COLLECTION;

  protected TypeCreator R = TypeCreator.REQUIRED;
  protected TypeCreator N = TypeCreator.NULLABLE;

  protected SubstraitBuilder b = new SubstraitBuilder(defaultExtensionCollection);
  protected ExtensionCollector functionCollector = new ExtensionCollector();
  protected RelProtoConverter relProtoConverter = new RelProtoConverter(functionCollector);
  protected ProtoRelConverter protoRelConverter =
      new ProtoRelConverter(functionCollector, defaultExtensionCollection);

  protected void verifyRoundTrip(Rel rel) {
    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(rel, relReturned);
  }

  /**
   * Assert that a literal/expression roundtrips correctly through Proto serialization. Uses default
   * (null) extension collection.
   */
  protected void assertLiteralRoundtrip(Expression.Literal literal) {
    assertLiteralRoundtrip(literal, null);
  }

  /**
   * Assert that a literal/expression roundtrips correctly through Proto serialization.
   *
   * @param literal the literal to roundtrip
   * @param extensions custom extension collection, or null to use no extensions
   */
  protected void assertLiteralRoundtrip(
      Expression.Literal literal,
      SimpleExtension.@org.jspecify.annotations.Nullable ExtensionCollection extensions) {
    ExpressionProtoConverter toProto =
        new ExpressionProtoConverter(functionCollector, relProtoConverter);
    ProtoExpressionConverter fromProto =
        new ProtoExpressionConverter(functionCollector, extensions, EMPTY_TYPE, protoRelConverter);

    io.substrait.proto.Expression protoExpr =
        literal.accept(toProto, EmptyVisitationContext.INSTANCE);
    io.substrait.proto.Expression.Literal protoLiteral = protoExpr.getLiteral();
    Expression.Literal roundtripped = fromProto.from(protoLiteral);

    assertEquals(literal, roundtripped);
  }

  /**
   * Assert that an expression roundtrips correctly through Proto serialization. Uses default (null)
   * extension collection.
   */
  protected void assertExpressionRoundtrip(Expression expression) {
    assertExpressionRoundtrip(expression, null);
  }

  /**
   * Assert that an expression roundtrips correctly through Proto serialization.
   *
   * @param expression the expression to roundtrip
   * @param extensions custom extension collection, or null to use no extensions
   */
  protected void assertExpressionRoundtrip(
      Expression expression,
      SimpleExtension.@org.jspecify.annotations.Nullable ExtensionCollection extensions) {
    ExpressionProtoConverter toProto =
        new ExpressionProtoConverter(functionCollector, relProtoConverter);
    ProtoExpressionConverter fromProto =
        new ProtoExpressionConverter(functionCollector, extensions, EMPTY_TYPE, protoRelConverter);

    io.substrait.proto.Expression protoExpression =
        expression.accept(toProto, EmptyVisitationContext.INSTANCE);
    Expression roundtripped = fromProto.from(protoExpression);

    assertEquals(expression, roundtripped);
  }
}
