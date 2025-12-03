package io.substrait.type.proto;

import static io.substrait.expression.proto.ProtoExpressionConverter.EMPTY_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.util.EmptyVisitationContext;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class LiteralRoundtripTest extends TestBase {

  @Test
  void decimal() {
    final io.substrait.expression.Expression.DecimalLiteral val =
        ExpressionCreator.decimal(false, BigDecimal.TEN, 10, 2);
    final ExpressionProtoConverter to = new ExpressionProtoConverter(null, null);
    final ProtoExpressionConverter from =
        new ProtoExpressionConverter(null, null, EMPTY_TYPE, protoRelConverter);
    assertEquals(val, from.from(val.accept(to, EmptyVisitationContext.INSTANCE)));
  }
}
