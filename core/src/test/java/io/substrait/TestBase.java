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

  protected ExpressionProtoConverter expressionProtoConverter =
      new ExpressionProtoConverter(functionCollector, relProtoConverter);

  protected ProtoExpressionConverter protoExpressionConverter =
      new ProtoExpressionConverter(
          functionCollector, defaultExtensionCollection, EMPTY_TYPE, protoRelConverter);

  protected void verifyRoundTrip(Rel rel) {
    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(rel, relReturned);
  }

  protected void verifyRoundTrip(Expression expression) {
    io.substrait.proto.Expression protoExpression = expressionProtoConverter.toProto(expression);
    Expression expressionReturned = protoExpressionConverter.from(protoExpression);
    assertEquals(expression, expressionReturned);
  }
}
