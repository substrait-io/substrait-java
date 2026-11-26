package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.TestBase;
import io.substrait.expression.proto.NestingDetacher;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.AdvancedExtension;
import io.substrait.proto.Expression;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import java.util.List;
import org.junit.jupiter.api.Test;

class DetachedRelationWriterTest extends TestBase {
  private final io.substrait.relation.Rel scan =
      sb.namedScan(List.of("orders"), List.of("id"), List.of(R.I32));
  private final Expression condition = expressionProtoConverter.toProto(sb.bool(true));

  @Test
  void aDeepDetachedInputStillParsesAfterWritingThePlan() throws InvalidProtocolBufferException {
    Plan template = new PlanProtoConverter().toProto(sb.plan(sb.root(scan)));
    PlanRel.Builder root = template.getRelations(0).toBuilder();
    Rel leaf = root.getRoot().getInput();
    root.getRootBuilder().setInput(ordinal(0));
    for (int index = 0; index < 60; index++) {
      root.addDetachedRels(filter(ordinal(index + 1)));
    }
    root.addDetachedRels(leaf);
    Plan wire = template.toBuilder().setRelations(0, root).build();
    io.substrait.plan.Plan model =
        new ProtoPlanConverter().from(Plan.parseFrom(wire.toByteArray()));

    Plan output = new PlanProtoConverter().toProto(model);
    assertTrue(output.getRelations(0).getDetachedRelsCount() > 0);
    assertEquals(model, new ProtoPlanConverter().from(Plan.parseFrom(output.toByteArray())));

    io.substrait.plan.Plan earlier =
        io.substrait.plan.Plan.builder()
            .from(model)
            .version(
                io.substrait.plan.Plan.Version.builder()
                    .major(0)
                    .minor(106)
                    .patch(1)
                    .gitHash("older")
                    .producer("producer")
                    .build())
            .build();
    io.substrait.proto.Version version = new PlanProtoConverter().toProto(earlier).getVersion();
    assertEquals(107, version.getMinorNumber());
    assertEquals(0, version.getPatchNumber());
    assertEquals("", version.getGitHash());
    assertEquals("producer", version.getProducer());
  }

  @Test
  void detachingASubqueryKeepsTheOrdinalsInOpaquePayloads() throws InvalidProtocolBufferException {
    Rel target = relProtoConverter.toProto(scan);
    Rel deep = target;
    for (int i = 0; i < 60; i++) {
      deep = filter(deep);
    }
    Expression subquery =
        Expression.newBuilder()
            .setSubquery(
                Expression.Subquery.newBuilder()
                    .setScalar(Expression.Subquery.Scalar.newBuilder().setInput(deep)))
            .build();
    ExtendedExpression wire =
        ExtendedExpression.newBuilder()
            .setBaseSchema(target.getRead().getBaseSchema())
            .addReferredExpr(ExpressionReference.newBuilder().setExpression(subquery))
            .addDetachedRels(target)
            .setAdvancedExtensions(
                AdvancedExtension.newBuilder().setEnhancement(Any.pack(ordinal(0))))
            .build();

    ExtendedExpression bounded = NestingDetacher.withMaxDepth(20).detach(wire);
    ExtendedExpression parsed = ExtendedExpression.parseFrom(bounded.toByteArray());
    assertTrue(parsed.getDetachedRelsCount() > 1);
    Rel reference = parsed.getAdvancedExtensions().getEnhancement().unpack(Rel.class);
    assertEquals(target, parsed.getDetachedRels(reference.getDetachedRelOrdinal()));
    assertEquals(
        new io.substrait.extendedexpression.ProtoExtendedExpressionConverter().from(wire),
        new io.substrait.extendedexpression.ProtoExtendedExpressionConverter().from(parsed));
  }

  private static Rel ordinal(int index) {
    return Rel.newBuilder().setDetachedRelOrdinal(index).build();
  }

  private Rel filter(Rel input) {
    return Rel.newBuilder()
        .setFilter(FilterRel.newBuilder().setInput(input).setCondition(condition))
        .build();
  }
}
