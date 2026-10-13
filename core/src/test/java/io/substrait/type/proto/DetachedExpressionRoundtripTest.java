package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.LambdaBuilder;
import io.substrait.expression.proto.NestingDetacher;
import io.substrait.extendedexpression.ExtendedExpression;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ImmutableExpressionReference;
import io.substrait.extendedexpression.ImmutableExtendedExpression;
import io.substrait.extendedexpression.ProtoExtendedExpressionConverter;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionProtoConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.PlanRel;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.type.NamedStruct;
import io.substrait.utils.RelSamples;
import io.substrait.utils.StringHolderHandlingProtoRelConverter;
import io.substrait.utils.StringHolderHandlingRelProtoConverter;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies detached expression subtrees (spec v0.105.0): the writer moves expressions into {@code
 * detached_expressions} only when they nest too deeply, the reader resolves the ordinals back into
 * the same expressions, and the reader enforces the rules the spec places on the references.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DetachedExpressionRoundtripTest extends StringHolderRoundtripTestBase {

  /** A detacher with no depth to spare, so it detaches every expression it can. */
  static final NestingDetacher DETACH_ALL = NestingDetacher.withMaxDepth(0);

  /** Deep enough that the nested additions exceed protobuf-java's parse recursion limit. */
  static final int DEEP = 200;

  final Map<Class<? extends Rel>, Rel> relSamples = new RelSamples(sb, extensions).samples();

  final NamedScan scan = sb.namedScan(List.of("t"), List.of("a"), List.of(R.I32));

  final Rel orders =
      sb.namedScan(List.of("orders"), List.of("o_orderkey", "o_custkey"), List.of(R.I64, R.I64));

  final Rel customer =
      sb.namedScan(List.of("customer"), List.of("c_custkey", "c_nationkey"), List.of(R.I64, R.I64));

  Stream<Arguments> samples() {
    return relSamples.entrySet().stream()
        .map(sample -> Arguments.of(sample.getKey().getSimpleName(), sample.getValue()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("samples")
  void everyRelationRoundtripsWithEveryExpressionDetached(String relationType, Rel rel) {
    io.substrait.proto.Rel protoRel =
        new StringHolderHandlingRelProtoConverter(functionCollector).toProto(rel);
    PlanRel detached =
        DETACH_ALL.detach(
            PlanRel.newBuilder()
                .setRoot(io.substrait.proto.RelRoot.newBuilder().setInput(protoRel))
                .build());

    if (containsExpression(protoRel)) {
      assertTrue(detached.getDetachedExpressionsCount() > 0, "no expression was detached");
      assertNotEquals(protoRel, detached.getRoot().getInput());
    }
    Rel returned =
        new StringHolderHandlingProtoRelConverter(functionCollector, extensions)
            .from(detached)
            .getInput();
    assertEquals(rel, returned);
  }

  @Test
  void aShallowPlanIsLeftUnchanged() {
    Plan plan = sb.plan(sb.root(sb.project(input -> List.of(nestedAdditions(input, 3)), scan)));
    io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);
    assertEquals(0, proto.getRelations(0).getDetachedExpressionsCount());
    assertEquals(proto, planConverter(NestingDetacher.disabled()).toProto(plan));
  }

  @Test
  void aDeepPlanIsDetachedToParseWithDefaultLimits() throws InvalidProtocolBufferException {
    Plan plan = sb.plan(sb.root(sb.project(input -> List.of(nestedAdditions(input, DEEP)), scan)));

    byte[] undetached = planConverter(NestingDetacher.disabled()).toProto(plan).toByteArray();
    assertThrows(
        InvalidProtocolBufferException.class, () -> io.substrait.proto.Plan.parseFrom(undetached));

    io.substrait.proto.Plan proto =
        io.substrait.proto.Plan.parseFrom(new PlanProtoConverter().toProto(plan).toByteArray());
    assertTrue(proto.getRelations(0).getDetachedExpressionsCount() > 0);
    assertTrue(depth(proto) <= NestingDetacher.DEFAULT_MAX_DEPTH, () -> "depth " + depth(proto));
    assertEquals(plan, new ProtoPlanConverter().from(proto));
  }

  @Test
  void aDeepExtendedExpressionIsDetachedToParseWithDefaultLimits()
      throws InvalidProtocolBufferException {
    ExtendedExpression extendedExpression =
        ImmutableExtendedExpression.builder()
            .baseSchema(NamedStruct.of(List.of("a"), R.struct(R.I32)))
            .addReferredExpressions(
                ImmutableExpressionReference.builder()
                    .expression(nestedAdditions(scan, DEEP))
                    .addOutputNames("deep")
                    .build())
            .build();

    io.substrait.proto.ExtendedExpression proto =
        io.substrait.proto.ExtendedExpression.parseFrom(
            new ExtendedExpressionProtoConverter().toProto(extendedExpression).toByteArray());
    assertTrue(proto.getDetachedExpressionsCount() > 0);
    assertTrue(depth(proto) <= NestingDetacher.DEFAULT_MAX_DEPTH, () -> "depth " + depth(proto));
    assertEquals(extendedExpression, new ProtoExtendedExpressionConverter().from(proto));

    io.substrait.proto.ExtendedExpression all =
        new ExtendedExpressionProtoConverter(DETACH_ALL).toProto(extendedExpression);
    assertEquals(extendedExpression, new ProtoExtendedExpressionConverter().from(all));
  }

  @Test
  void aContainerWithinTheLimitIsReturnedAsIs() {
    PlanRel planRel =
        PlanRel.newBuilder()
            .setRoot(
                io.substrait.proto.RelRoot.newBuilder()
                    .setInput(relProtoConverter.toProto(filterOn(sb.bool(true)))))
            .build();
    assertSame(planRel, NestingDetacher.defaultDetacher().detach(planRel));
  }

  @Test
  void anUnreferencedEntryIsNeitherConvertedNorValidated() {
    PlanRel planRel =
        DETACH_ALL.detach(planRel(filterOn(sb.equal(sb.fieldReference(scan, 0), sb.i32(1)))));
    PlanRel withUnreachable =
        planRel.toBuilder()
            .addDetachedExpressions(ordinal(0))
            .addDetachedExpressions(io.substrait.proto.Expression.getDefaultInstance())
            .build();
    assertEquals(protoRelConverter.from(planRel), protoRelConverter.from(withUnreachable));
  }

  @Test
  void anOrdinalOutOfRangeIsRejected() {
    assertRejected("out of range", condition(ordinal(1)), List.of(literal()));
    assertRejected("out of range", condition(ordinal(0)), List.of());
  }

  @Test
  void anEntryReferencedTwiceIsRejected() {
    assertRejected(
        "referenced more than once", condition(equal(ordinal(0), ordinal(0))), List.of(literal()));
  }

  @Test
  void aCycleIsRejected() {
    assertRejected(
        "cycle",
        condition(ordinal(0)),
        List.of(equal(ordinal(1), literal()), equal(ordinal(0), literal())));
  }

  @Test
  void anEntryThatIsItselfAnOrdinalIsRejected() {
    assertRejected(
        "itself a detached expression ordinal",
        condition(ordinal(0)),
        List.of(ordinal(1), literal()));
  }

  @Test
  void anOrdinalOutsideAContainerIsRejected() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class, () -> protoRelConverter.from(condition(ordinal(0))));
    assertTrue(error.getMessage().contains("outside a PlanRel"), error.getMessage());
  }

  @Test
  void aPlanRelWithoutARootIsRejected() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                protoRelConverter.from(
                    PlanRel.newBuilder().setRel(relProtoConverter.toProto(scan)).build()));
    assertTrue(error.getMessage().contains("Only PlanRel roots"), error.getMessage());
  }

  @Test
  void aLambdaBodyResolvesInTheLambdaScope() {
    LambdaBuilder lb = new LambdaBuilder();
    Rel input = sb.namedScan(List.of("t"), List.of("xs", "k"), List.of(R.list(R.I64), R.I64));
    assertDetachedRoundTrip(
        sb.project(
            in ->
                List.of(
                    sb.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_LIST,
                        "transform:list_func",
                        R.list(R.I64),
                        sb.fieldReference(in, 0),
                        lb.lambda(
                            List.of(R.I64),
                            p -> sb.add(sb.add(p.ref(0), sb.fieldReference(in, 1)), sb.i64(1))))),
            Remap.of(List.of(2)),
            input));
  }

  @Test
  void aNestedLambdaBodyResolvesInItsOwnScope() {
    LambdaBuilder lb = new LambdaBuilder();
    Rel input = sb.namedScan(List.of("t"), List.of("xs"), List.of(R.list(R.list(R.I64))));
    assertDetachedRoundTrip(
        sb.project(
            in ->
                List.of(
                    sb.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_LIST,
                        "transform:list_func",
                        R.list(R.list(R.I64)),
                        sb.fieldReference(in, 0),
                        lb.lambda(
                            List.of(R.list(R.I64)),
                            outer ->
                                sb.scalarFn(
                                    DefaultExtensionCatalog.FUNCTIONS_LIST,
                                    "transform:list_func",
                                    R.list(R.I64),
                                    outer.ref(0),
                                    lb.lambda(
                                        List.of(R.I64),
                                        inner -> sb.add(inner.ref(0), sb.i64(1))))))),
            Remap.of(List.of(1)),
            input));
  }

  @Test
  void aStepsOutReferenceResolvesAgainstTheEnclosingQuery() {
    Rel subquery =
        correlated(FieldReference.newRootStructOuterReference(1, orders.getRecordType(), 1));
    assertDetachedRoundTrip(
        sb.project(
            input -> List.of(sb.fieldReference(input, 0), sb.scalarSubquery(subquery, R.I64)),
            Remap.of(List.of(2, 3)),
            orders));
  }

  @Test
  void aRelReferenceResolvesAgainstTheAnchoredRelation() {
    Rel anchored = orders.withRelAnchor(1);
    Rel subquery =
        correlated(
            FieldReference.newRootStructOuterReferenceByRelReference(
                1, anchored.getRecordType(), 1));
    assertDetachedRoundTrip(
        sb.project(
            input -> List.of(sb.fieldReference(input, 0), sb.scalarSubquery(subquery, R.I64)),
            Remap.of(List.of(2, 3)),
            anchored));
  }

  @Test
  void inPredicateNeedlesAndACorrelatedHaystackResolveInTheirScopes() {
    Rel haystack =
        sb.project(
            input -> List.of(sb.fieldReference(input, 0)),
            Remap.of(List.of(2)),
            sb.filter(
                input ->
                    sb.equal(
                        sb.fieldReference(input, 1),
                        FieldReference.newRootStructOuterReference(1, orders.getRecordType(), 1)),
                customer));
    assertDetachedRoundTrip(
        sb.filter(
            input ->
                sb.inPredicate(
                    haystack,
                    sb.add(sb.fieldReference(input, 0), sb.i64(1)),
                    sb.fieldReference(input, 1)),
            orders));
  }

  /** Round-trips {@code rel} as a plan with every expression detached. */
  private void assertDetachedRoundTrip(Rel rel) {
    Plan plan = sb.plan(sb.root(rel));
    io.substrait.proto.Plan proto = planConverter(DETACH_ALL).toProto(plan);
    assertTrue(proto.getRelations(0).getDetachedExpressionsCount() > 0);
    assertEquals(plan, new ProtoPlanConverter().from(proto));
  }

  /** A subquery over customer whose filter compares c_custkey with the given outer reference. */
  private Rel correlated(FieldReference outerReference) {
    return sb.project(
        input -> List.of(sb.fieldReference(input, 1)),
        Remap.of(List.of(2)),
        sb.filter(input -> sb.equal(sb.fieldReference(input, 0), outerReference), customer));
  }

  private void assertRejected(
      String message,
      io.substrait.proto.Rel rel,
      List<io.substrait.proto.Expression> detachedExpressions) {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                protoRelConverter.from(
                    PlanRel.newBuilder()
                        .setRoot(io.substrait.proto.RelRoot.newBuilder().setInput(rel))
                        .addAllDetachedExpressions(detachedExpressions)
                        .build()));
    assertTrue(error.getMessage().contains(message), error.getMessage());
  }

  private Rel filterOn(Expression condition) {
    return sb.filter(input -> condition, scan);
  }

  private PlanRel planRel(Rel rel) {
    return PlanRel.newBuilder()
        .setRoot(io.substrait.proto.RelRoot.newBuilder().setInput(relProtoConverter.toProto(rel)))
        .build();
  }

  /** A filter over {@link #scan} with the given proto condition. */
  private io.substrait.proto.Rel condition(io.substrait.proto.Expression condition) {
    io.substrait.proto.Rel rel = relProtoConverter.toProto(filterOn(sb.bool(true)));
    return rel.toBuilder().setFilter(rel.getFilter().toBuilder().setCondition(condition)).build();
  }

  /** An {@code equal:any_any} of the two proto operands, shaped as the writer emits it. */
  private io.substrait.proto.Expression equal(
      io.substrait.proto.Expression left, io.substrait.proto.Expression right) {
    io.substrait.proto.Expression template =
        expressionProtoConverter.toProto(sb.equal(sb.i32(1), sb.i32(1)));
    io.substrait.proto.Expression.ScalarFunction.Builder function =
        template.getScalarFunction().toBuilder();
    function.getArgumentsBuilder(0).setValue(left);
    function.getArgumentsBuilder(1).setValue(right);
    return template.toBuilder().setScalarFunction(function).build();
  }

  private io.substrait.proto.Expression literal() {
    return expressionProtoConverter.toProto(sb.i32(1));
  }

  private static io.substrait.proto.Expression ordinal(int ordinal) {
    return io.substrait.proto.Expression.newBuilder().setDetachedExpressionOrdinal(ordinal).build();
  }

  private Expression nestedAdditions(Rel input, int depth) {
    Expression expression = sb.fieldReference(input, 0);
    for (int i = 0; i < depth; i++) {
      expression = sb.add(expression, ExpressionCreator.i32(false, i));
    }
    return expression;
  }

  private static PlanProtoConverter planConverter(NestingDetacher detacher) {
    return new PlanProtoConverter(
        DefaultExtensionCatalog.DEFAULT_COLLECTION, new ExtensionProtoConverter<>(), detacher);
  }

  private static boolean containsExpression(Message message) {
    if (message instanceof io.substrait.proto.Expression) {
      return true;
    }
    return children(message).anyMatch(DetachedExpressionRoundtripTest::containsExpression);
  }

  /** Returns how many levels of messages nest below {@code message}, as a parser counts them. */
  private static int depth(Message message) {
    return children(message).mapToInt(child -> 1 + depth(child)).max().orElse(0);
  }

  private static Stream<Message> children(Message message) {
    return message.getAllFields().entrySet().stream()
        .filter(field -> field.getKey().getJavaType() == FieldDescriptor.JavaType.MESSAGE)
        .flatMap(
            field ->
                field.getKey().isRepeated()
                    ? ((List<?>) field.getValue()).stream().map(Message.class::cast)
                    : Stream.of((Message) field.getValue()));
  }
}
