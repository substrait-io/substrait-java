package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.expression.FieldReference;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ImmutableExtensionLookup;
import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.ComparisonJoinKey;
import io.substrait.proto.ExecutionBehavior;
import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.HashJoinRel;
import io.substrait.proto.MergeJoinRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURN;
import io.substrait.proto.Version;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class CustomComparisonPlanRoundtripTest extends TestBase {

  private static final int POST_FILTER_REFERENCE = 99;

  private final SimpleExtension.ScalarFunctionVariant equal =
      extensions.getScalarFunction(
          SimpleExtension.FunctionAnchor.of(
              DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "equal:any_any"));

  private static Stream<Arguments> joinCases() {
    return Stream.of(false, true)
        .flatMap(
            merge ->
                // Both zero and the unsigned value represented by -1 are valid wire anchors.
                Stream.of(0, 1, 42, -1)
                    .flatMap(
                        anchor ->
                            Stream.of(false, true)
                                .map(withFilter -> Arguments.of(merge, anchor, withFilter))));
  }

  @ParameterizedTest
  @MethodSource("joinCases")
  void preservesComparisonIdentity(boolean merge, int anchor, boolean withFilter) {
    Plan original = plan(merge, anchor, equal, withFilter);
    io.substrait.plan.Plan pojo = new ProtoPlanConverter().from(original);
    Plan converted = new PlanProtoConverter().toProto(pojo);

    assertComparisonDeclarations(converted, merge, equal, extensions);
    assertEquals(withFilter ? 2 : 1, converted.getExtensionsCount());
    if (withFilter) {
      Rel rel = converted.getRelations(0).getRoot().getInput();
      Expression filter =
          merge ? rel.getMergeJoin().getPostJoinFilter() : rel.getHashJoin().getPostJoinFilter();
      assertEquals(
          "not_equal:any_any",
          ImmutableExtensionLookup.builder()
              .from(converted)
              .build()
              .getScalarFunction(filter.getScalarFunction().getFunctionReference(), extensions)
              .key());
    }
    assertEquals(pojo, new ProtoPlanConverter().from(converted));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void usesConfiguredExtensionCollection(boolean merge) {
    SimpleExtension.ScalarFunctionVariant custom =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .from(equal)
            .urn("extension:example:comparisons")
            .name("matches")
            .build();
    SimpleExtension.ExtensionCollection collection =
        SimpleExtension.ExtensionCollection.builder().addScalarFunctions(custom).build();
    Plan original = plan(merge, 42, custom, false);
    io.substrait.plan.Plan pojo = new ProtoPlanConverter(collection).from(original);
    Plan converted = new PlanProtoConverter(collection).toProto(pojo);

    assertComparisonDeclarations(converted, merge, custom, collection);
    assertEquals(1, converted.getExtensionsCount());
    assertEquals(pojo, new ProtoPlanConverter(collection).from(converted));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsUndeclaredComparisonReference(boolean merge) {
    Plan original = plan(merge, 42, equal, false).toBuilder().clearExtensions().build();
    assertThrows(IllegalArgumentException.class, () -> new ProtoPlanConverter().from(original));
  }

  private void assertComparisonDeclarations(
      Plan plan,
      boolean merge,
      SimpleExtension.ScalarFunctionVariant expected,
      SimpleExtension.ExtensionCollection collection) {
    Rel rel = plan.getRelations(0).getRoot().getInput();
    List<ComparisonJoinKey> keys =
        merge ? rel.getMergeJoin().getKeysList() : rel.getHashJoin().getKeysList();
    assertEquals(2, keys.size());
    int reference = keys.get(0).getComparison().getCustomFunctionReference();
    assertEquals(reference, keys.get(1).getComparison().getCustomFunctionReference());
    assertEquals(
        expected,
        ImmutableExtensionLookup.builder()
            .from(plan)
            .build()
            .getScalarFunction(reference, collection));
  }

  private Plan plan(
      boolean merge,
      int comparisonAnchor,
      SimpleExtension.ScalarFunctionVariant comparison,
      boolean withFilter) {
    Rel input =
        relProtoConverter.toProto(
            sb.namedScan(Arrays.asList("t"), Arrays.asList("x"), Arrays.asList(R.I32)));
    ComparisonJoinKey key =
        ComparisonJoinKey.newBuilder()
            .setLeft(field(0).getSelection())
            .setRight(field(0).getSelection())
            .setComparison(
                ComparisonJoinKey.ComparisonType.newBuilder()
                    .setCustomFunctionReference(comparisonAnchor))
            .build();
    Expression postFilter =
        Expression.newBuilder()
            .setScalarFunction(
                Expression.ScalarFunction.newBuilder()
                    .setFunctionReference(POST_FILTER_REFERENCE)
                    .setOutputType(relProtoConverter.getTypeProtoConverter().toProto(R.BOOLEAN))
                    .addArguments(FunctionArgument.newBuilder().setValue(field(0)))
                    .addArguments(FunctionArgument.newBuilder().setValue(field(1))))
            .build();
    Rel.Builder relation = Rel.newBuilder();
    if (merge) {
      MergeJoinRel.Builder join =
          MergeJoinRel.newBuilder()
              .setLeft(input)
              .setRight(input)
              .setType(MergeJoinRel.JoinType.JOIN_TYPE_INNER)
              .addKeys(key)
              .addKeys(key);
      if (withFilter) {
        join.setPostJoinFilter(postFilter);
      }
      relation.setMergeJoin(join);
    } else {
      HashJoinRel.Builder join =
          HashJoinRel.newBuilder()
              .setLeft(input)
              .setRight(input)
              .setType(HashJoinRel.JoinType.JOIN_TYPE_INNER)
              .addKeys(key)
              .addKeys(key);
      if (withFilter) {
        join.setPostJoinFilter(postFilter);
      }
      relation.setHashJoin(join);
    }
    Plan.Builder plan =
        Plan.newBuilder()
            .setVersion(Version.newBuilder().setMinorNumber(102))
            .setExecutionBehavior(
                ExecutionBehavior.newBuilder()
                    .setVariableEvalMode(
                        ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN))
            .addExtensionUrns(
                SimpleExtensionURN.newBuilder().setExtensionUrnAnchor(1).setUrn(comparison.urn()))
            .addExtensions(function(comparisonAnchor, comparison.key()))
            .addRelations(
                PlanRel.newBuilder()
                    .setRoot(
                        RelRoot.newBuilder()
                            .setInput(relation)
                            .addNames("left_x")
                            .addNames("right_x")));
    if (withFilter) {
      plan.addExtensions(function(POST_FILTER_REFERENCE, "not_equal:any_any"));
    }
    return plan.build();
  }

  private Expression field(int index) {
    return expressionProtoConverter.toProto(FieldReference.newRootStructReference(index, R.I32));
  }

  private static SimpleExtensionDeclaration function(int reference, String name) {
    return SimpleExtensionDeclaration.newBuilder()
        .setExtensionFunction(
            SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                .setExtensionUrnReference(1)
                .setFunctionAnchor(reference)
                .setName(name))
        .build();
  }
}
