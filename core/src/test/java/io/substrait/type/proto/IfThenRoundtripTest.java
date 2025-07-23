package io.substrait.type.proto;

import static io.substrait.expression.proto.ProtoExpressionConverter.EMPTY_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.util.EmptyVisitationContext;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class IfThenRoundtripTest extends TestBase {

  @Test
  void ifThenNotNullable() {
    final Expression.IfThen ifRel =
        b.ifThen(
            Arrays.asList(
                b.ifClause(ExpressionCreator.bool(false, false), ExpressionCreator.i64(false, 1))),
            ExpressionCreator.i64(false, 2));
    assertFalse(ifRel.getType().nullable());

    ExpressionProtoConverter to = new ExpressionProtoConverter(null, null);
    ProtoExpressionConverter from =
        new ProtoExpressionConverter(null, null, EMPTY_TYPE, protoRelConverter);
    assertEquals(ifRel, from.from(ifRel.accept(to, EmptyVisitationContext.INSTANCE)));
  }

  @Test
  void ifThenNullable() {
    final Expression.IfThen ifRel =
        b.ifThen(
            Arrays.asList(
                b.ifClause(ExpressionCreator.bool(true, false), ExpressionCreator.i64(true, 1))),
            ExpressionCreator.i64(false, 2));
    assertTrue(ifRel.getType().nullable());

    ExpressionProtoConverter to = new ExpressionProtoConverter(null, null);
    ProtoExpressionConverter from =
        new ProtoExpressionConverter(null, null, EMPTY_TYPE, protoRelConverter);
    assertEquals(ifRel, from.from(ifRel.accept(to, EmptyVisitationContext.INSTANCE)));
  }
}
