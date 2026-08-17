package io.substrait;

import static io.substrait.expression.proto.ProtoExpressionConverter.EMPTY_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.Project;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.TypeCreator;
import java.io.IOException;

public abstract class TestBase {

  protected static final TypeCreator R = TypeCreator.REQUIRED;
  protected static final TypeCreator N = TypeCreator.NULLABLE;

  protected final SimpleExtension.ExtensionCollection extensions;

  protected ExtensionCollector functionCollector = new ExtensionCollector();
  protected RelProtoConverter relProtoConverter = new RelProtoConverter(functionCollector);

  protected SubstraitBuilder sb;
  protected ProtoRelConverter protoRelConverter;
  protected ExpressionProtoConverter expressionProtoConverter =
      relProtoConverter.getExpressionProtoConverter();

  protected ProtoExpressionConverter protoExpressionConverter;

  protected TestBase() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  protected TestBase(SimpleExtension.ExtensionCollection extensions) {
    this.extensions = extensions;
    this.sb = new SubstraitBuilder(extensions);
    this.protoRelConverter = new ProtoRelConverter(functionCollector, extensions);
    // Must be assigned here rather than in a field initializer: it needs the fully constructed
    // protoRelConverter to convert subqueries, and the extension collection this instance was
    // built with to resolve user-defined types.
    this.protoExpressionConverter =
        new ProtoExpressionConverter(functionCollector, extensions, EMPTY_TYPE, protoRelConverter);
  }

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

  /** Wraps the given expression in a {@link Project} over an empty virtual table scan. */
  protected Project projectOf(Expression expression) {
    return Project.builder().addExpressions(expression).input(sb.emptyVirtualTableScan()).build();
  }

  public static String asString(String resource) throws IOException {
    return Resources.toString(Resources.getResource(resource), Charsets.UTF_8);
  }
}
