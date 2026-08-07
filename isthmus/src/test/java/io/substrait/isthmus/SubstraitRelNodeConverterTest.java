package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionOption;
import io.substrait.expression.ImmutableAggregateFunctionInvocation;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.InvalidFunctionBindingException;
import io.substrait.extension.ResolvedAggregateBinding;
import io.substrait.extension.ResolvedArgument;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.relation.Join.JoinType;
import io.substrait.relation.Rel;
import io.substrait.relation.Set.SetOp;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SubstraitRelNodeConverterTest extends PlanTestBase {

  // Define a shared table (i.e. a NamedScan) for use in tests.
  final List<Type> commonTableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN);
  final List<Type> commonTableTypeTwice =
      Stream.concat(commonTableType.stream(), commonTableType.stream())
          .collect(Collectors.toList());
  final Rel commonTable =
      sb.namedScan(List.of("example"), List.of("a", "b", "c", "d"), commonTableType);

  @Nested
  class Aggregate {
    @Test
    void direct() {
      Plan.Root root =
          sb.root(
              sb.aggregate(
                  input -> sb.grouping(input, 0, 2),
                  input -> List.of(sb.count(input, 0)),
                  commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING, R.I64);

      // The declared COUNT type matches Calcite's inference, so the call keeps the stock operator
      // rather than a binding-carrying wrapper.
      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) relNode;
      assertEquals(
          Optional.empty(),
          AggregateFunctions.boundBinding(calciteAgg.getAggCallList().get(0).getAggregation()));
    }

    @Test
    void emit() {
      Plan.Root root =
          sb.root(
              sb.aggregate(
                  input -> List.of(sb.grouping(input, 0, 2)),
                  input -> List.of(sb.count(input, 0)),
                  Optional.of(sb.remap(1, 2)),
                  commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), N.STRING, R.I64);
    }

    @Test
    void declaredMeasureOutputTypes() {
      Rel aggregate =
          sb.aggregate(
              input -> sb.grouping(input, 2),
              input ->
                  List.of(
                      withOutputType(sb.sum(input, 0), R.I64),
                      withOutputType(sb.avg(input, 1), R.FP32)),
              commonTable);

      RelNode relNode = substraitToCalcite.convert(aggregate);
      assertRowMatch(relNode.getRowType(), N.STRING, R.I64, R.FP32);

      // Both declared types diverge from Calcite's inference, so both calls carry a resolved
      // binding on a wrapper (PLAN_OUTPUT preserves the plan's declared type).
      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) relNode;
      for (AggregateCall call : calciteAgg.getAggCallList()) {
        assertTrue(AggregateFunctions.boundBinding(call.getAggregation()).isPresent());
      }

      assertFullRoundTrip(aggregate);
    }

    @Test
    void declaredDecimalWidthIsPreserved() {
      Rel input =
          sb.namedScan(List.of("example"), List.of("d", "g"), List.of(R.decimal(10, 2), R.STRING));
      // The standard extension declarations for decimal sum and avg return DECIMAL<38,S>, while
      // Calcite's inference keeps the argument's precision. The declared width must survive.
      Rel aggregate =
          sb.aggregate(
              i -> sb.grouping(i, 1),
              i ->
                  List.of(
                      sb.measure(
                          sb.aggregateFn(
                              DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL,
                              "sum:dec",
                              N.decimal(38, 2),
                              sb.fieldReference(i, 0))),
                      sb.measure(
                          sb.aggregateFn(
                              DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL,
                              "avg:dec",
                              N.decimal(38, 2),
                              sb.fieldReference(i, 0)))),
              input);

      RelNode relNode = substraitToCalcite.convert(aggregate);
      assertRowMatch(relNode.getRowType(), R.STRING, N.decimal(38, 2), N.decimal(38, 2));
      assertFullRoundTrip(aggregate);
    }

    @Test
    void declaredTypeOnGlobalAggregateWithoutGroupings() {
      Rel input = sb.namedScan(List.of("example"), List.of("a"), List.of(R.I32));
      // An aggregate with no groupings at all (as opposed to one empty grouping) is a global
      // aggregation; the declared type must be preserved for it as well.
      Rel aggregate =
          io.substrait.relation.Aggregate.builder()
              .input(input)
              .measures(List.of(withOutputType(sb.sum(input, 0), R.I64)))
              .build();

      RelNode relNode = substraitToCalcite.convert(aggregate);
      assertRowMatch(relNode.getRowType(), R.I64);

      // A global aggregation is one empty grouping set, not zero grouping sets. RelBuilder derives
      // each measure's hasEmptyGroup from the grouping sets, so anything else would re-infer the
      // measures against a group key the aggregate does not have.
      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) relNode;
      assertEquals(List.of(ImmutableBitSet.of()), calciteAgg.getGroupSets());
      assertEquals(org.apache.calcite.rel.core.Aggregate.Group.SIMPLE, calciteAgg.getGroupType());

      // Converting back therefore normalizes "no groupings at all" to the equivalent single empty
      // grouping — the same global aggregation, spelled the way Calcite spells it.
      io.substrait.relation.Aggregate roundTripped =
          (io.substrait.relation.Aggregate) SubstraitRelVisitor.convert(relNode, converterProvider);
      assertEquals(1, roundTripped.getGroupings().size());
      assertTrue(roundTripped.getGroupings().get(0).getExpressions().isEmpty());
    }

    @Test
    void boundOperatorIdentityIgnoresDeclaredType() {
      ResolvedAggregateBinding binding =
          ResolvedAggregateBinding.resolve(sb.sum(commonTable, 0).getFunction());
      RelDataType i64 = converterProvider.getTypeConverter().toCalcite(typeFactory, N.I64);
      RelDataType fp64 = converterProvider.getTypeConverter().toCalcite(typeFactory, N.FP64);

      SqlAggFunction boundToI64 = AggregateFunctions.bind(AggregateFunctions.SUM, binding, i64);
      SqlAggFunction boundToFp64 = AggregateFunctions.bind(AggregateFunctions.SUM, binding, fp64);

      // Operator identity answers "which Substrait function is this?", never "which type does it
      // produce?", so matching on the operator stays type-agnostic.
      assertEquals(boundToI64, boundToFp64);
      assertEquals(boundToI64.hashCode(), boundToFp64.hashCode());

      // The carried type is still readable, it is just not part of the identity.
      assertEquals(Optional.of(i64), AggregateFunctions.declaredOutputType(boundToI64));
      assertEquals(Optional.of(fp64), AggregateFunctions.declaredOutputType(boundToFp64));
      assertEquals(Optional.empty(), AggregateFunctions.declaredOutputType(AggregateFunctions.SUM));

      // A different Substrait function is a different operator.
      ResolvedAggregateBinding countBinding =
          ResolvedAggregateBinding.resolve(sb.count(commonTable, 0).getFunction());
      assertNotEquals(
          boundToI64, AggregateFunctions.bind(AggregateFunctions.SUM, countBinding, i64));
    }

    @Test
    void enumArgumentDistinguishesMeasuresOfTheSameFunction() {
      Rel input = sb.namedScan(List.of("example"), List.of("x", "g"), List.of(R.FP32, R.STRING));
      // Both measures call std_dev:req_fp32 on the same column; only the leading "distribution"
      // enum argument differs, so only the resolved binding tells them apart.
      Rel aggregate =
          sb.aggregate(
              i -> sb.grouping(i, 1),
              i -> List.of(stdDev(i, "POPULATION"), stdDev(i, "SAMPLE")),
              input);

      RelNode relNode = substraitToCalcite.convert(aggregate);
      assertRowMatch(relNode.getRowType(), R.STRING, R.FP32, R.FP32);

      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) relNode;
      List<AggregateCall> calls = calciteAgg.getAggCallList();
      assertEquals(2, calls.size());
      ResolvedAggregateBinding population = boundBinding(calls.get(0));
      ResolvedAggregateBinding sample = boundBinding(calls.get(1));
      assertNotEquals(population, sample);
      assertEquals(Optional.of("POPULATION"), enumArgument(population));
      assertEquals(Optional.of("SAMPLE"), enumArgument(sample));
    }

    @Test
    void functionOptionsSurviveConversion() {
      Rel input = sb.namedScan(List.of("example"), List.of("a", "g"), List.of(R.I32, R.STRING));
      // count(i32) -> i64 matches Calcite's inference exactly, so nothing about the type forces a
      // wrapper — but the plan's "overflow" option has no place in a Calcite aggregate call, and
      // dropping it would silently change what the plan asks for.
      Rel aggregate = sb.aggregate(i -> sb.grouping(i, 1), i -> List.of(count(i, "ERROR")), input);

      RelNode relNode = substraitToCalcite.convert(aggregate);

      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) relNode;
      ResolvedAggregateBinding binding = boundBinding(calciteAgg.getAggCallList().get(0));
      assertEquals(Optional.of(List.of("ERROR")), binding.function().option("overflow"));

      // ... and the option comes back out on the way to Substrait, instead of being re-matched
      // away.
      assertFullRoundTrip(aggregate);
    }

    @Test
    void measuresDifferingOnlyByOptionsStayDistinct() {
      Rel input = sb.namedScan(List.of("example"), List.of("a", "g"), List.of(R.I32, R.STRING));
      Rel aggregate =
          sb.aggregate(
              i -> sb.grouping(i, 1), i -> List.of(count(i, "ERROR"), count(i, "SILENT")), input);

      RelNode relNode = substraitToCalcite.convert(aggregate);

      // Both measures are COUNT(a) of type i64 and differ only in an option Calcite cannot express,
      // so only the carried binding keeps them from being deduplicated into one column.
      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) relNode;
      assertEquals(2, calciteAgg.getAggCallList().size());
      assertNotEquals(
          boundBinding(calciteAgg.getAggCallList().get(0)),
          boundBinding(calciteAgg.getAggCallList().get(1)));
      assertRowMatch(relNode.getRowType(), R.STRING, R.I64, R.I64);
    }

    @Test
    void intermediatePhaseSurvivesBothDirections() {
      Rel input = sb.namedScan(List.of("example"), List.of("partial"), List.of(R.I64));
      // count accumulates into i64, and a final phase consumes that state. Calcite has no notion of
      // aggregation phases, so only the carried binding can bring this back: matching would see
      // COUNT(i64) and rebuild a full initial-to-result aggregation instead.
      Rel aggregate =
          sb.aggregate(
              i -> sb.grouping(i),
              i -> List.of(count(i, null, Expression.AggregationPhase.INTERMEDIATE_TO_RESULT)),
              input);

      RelNode relNode = substraitToCalcite.convert(aggregate);

      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) relNode;
      assertEquals(
          Expression.AggregationPhase.INTERMEDIATE_TO_RESULT,
          boundBinding(calciteAgg.getAggCallList().get(0)).phase());

      assertFullRoundTrip(aggregate);
    }

    @Test
    void parameterizedIntermediateStateSurvivesBothDirections() {
      // avg:dec accumulates into STRUCT<DECIMAL<38,S>,i64>. A phase that consumes that state sees
      // only the state, so S cannot be bound and the intermediate type cannot be re-derived — the
      // binding has to be trusted as recorded instead of re-validated against the declaration.
      Type state = R.struct(R.decimal(38, 2), R.I64);
      // A NamedStruct names nested fields too, so the state column contributes three names.
      Rel input =
          sb.namedScan(List.of("example"), List.of("partial", "total", "count"), List.of(state));
      Rel aggregate =
          sb.aggregate(
              i -> sb.grouping(i),
              i ->
                  List.of(
                      io.substrait.relation.Aggregate.Measure.builder()
                          .function(
                              AggregateFunctionInvocation.builder()
                                  .declaration(
                                      extensions.getAggregateFunction(
                                          SimpleExtension.FunctionAnchor.of(
                                              DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL,
                                              "avg:dec")))
                                  .outputType(N.decimal(38, 2))
                                  .aggregationPhase(
                                      Expression.AggregationPhase.INTERMEDIATE_TO_RESULT)
                                  .invocation(Expression.AggregationInvocation.ALL)
                                  .addArguments(sb.fieldReference(i, 0))
                                  .build())
                          .build()),
              input);

      RelNode relNode = substraitToCalcite.convert(aggregate);

      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) relNode;
      assertEquals(
          Expression.AggregationPhase.INTERMEDIATE_TO_RESULT,
          boundBinding(calciteAgg.getAggCallList().get(0)).phase());

      assertFullRoundTrip(aggregate);
    }

    @Test
    void calciteInferenceIgnoresAnUnrepresentablePlanType() {
      // The declared type is a user-defined type this converter has no mapping for. Ignoring it is
      // exactly what CALCITE_INFERENCE promises, so the conversion must not fail on it.
      Rel aggregate =
          sb.aggregate(
              input -> sb.grouping(input, 2),
              input ->
                  List.of(
                      withOutputType(
                          sb.sum(input, 0),
                          sb.userDefinedType("extension:test:unmapped", "opaque"))),
              commonTable);
      SubstraitToCalcite calciteInference =
          new SubstraitToCalcite(
              converterProvider,
              null,
              new AggregateConversion(
                  AggregateConversion.OutputTypeSource.CALCITE_INFERENCE,
                  AggregateConversion.FunctionBindingValidation.NONE));

      assertRowMatch(calciteInference.convert(aggregate).getRowType(), N.STRING, N.I32);

      // Preserving it, on the other hand, is impossible — and says so instead of substituting.
      assertThrows(
          UnsupportedOperationException.class, () -> substraitToCalcite.convert(aggregate));
    }

    @Test
    void unwrappingRestoresTheDelegateAndKeepsTheType() {
      Rel aggregate =
          sb.aggregate(
              i -> sb.grouping(i, 2),
              i -> List.of(withOutputType(sb.sum(i, 0), R.I64)),
              commonTable);
      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) substraitToCalcite.convert(aggregate);
      AggregateCall bound = calciteAgg.getAggCallList().get(0);
      assertTrue(AggregateFunctions.boundBinding(bound.getAggregation()).isPresent());

      // What a consumer does before executing a converted plan: Calcite dispatches aggregate
      // implementations by operator identity, and a bound operator has none.
      AggregateCall unwrapped = AggregateFunctions.unwrapBound(bound);
      assertEquals(AggregateFunctions.SUM, unwrapped.getAggregation());
      assertEquals(bound.getType(), unwrapped.getType());
      assertEquals(Optional.empty(), AggregateFunctions.boundBinding(unwrapped.getAggregation()));
      // A call that carries no binding is returned untouched.
      assertSame(unwrapped, AggregateFunctions.unwrapBound(unwrapped));
    }

    @Test
    void converterFactoryDispatchDependsOnTheConfiguration() {
      List<String> factories = new ArrayList<>();
      ConverterProvider provider =
          new ConverterProvider() {
            @Override
            public SubstraitRelNodeConverter getSubstraitRelNodeConverter(RelBuilder relBuilder) {
              factories.add("single-argument");
              return super.getSubstraitRelNodeConverter(relBuilder);
            }

            @Override
            public SubstraitRelNodeConverter getSubstraitRelNodeConverter(
                RelBuilder relBuilder, AggregateConversion aggregateConversion) {
              factories.add("aggregate-aware");
              return super.getSubstraitRelNodeConverter(relBuilder, aggregateConversion);
            }
          };
      Rel aggregate =
          sb.aggregate(
              input -> sb.grouping(input, 2), input -> List.of(sb.count(input, 0)), commonTable);

      // The default configuration goes through the long-standing single-argument factory, so a
      // subclass that only overrides that one still customizes conversion.
      new SubstraitToCalcite(provider).convert(aggregate);
      assertEquals(List.of("single-argument", "aggregate-aware"), factories);

      // A non-default configuration cannot: it has nowhere to pass the configuration but the
      // two-argument factory.
      factories.clear();
      new SubstraitToCalcite(
              provider,
              null,
              new AggregateConversion(
                  AggregateConversion.OutputTypeSource.CALCITE_INFERENCE,
                  AggregateConversion.FunctionBindingValidation.NONE))
          .convert(aggregate);
      assertEquals(List.of("aggregate-aware"), factories);
    }

    private io.substrait.relation.Aggregate.Measure count(Rel input, String overflow) {
      return count(input, overflow, Expression.AggregationPhase.INITIAL_TO_RESULT);
    }

    private io.substrait.relation.Aggregate.Measure count(
        Rel input, String overflow, Expression.AggregationPhase phase) {
      SimpleExtension.AggregateFunctionVariant declaration =
          extensions.getAggregateFunction(
              SimpleExtension.FunctionAnchor.of(
                  DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC, "count:any"));
      ImmutableAggregateFunctionInvocation.Builder function =
          AggregateFunctionInvocation.builder()
              .declaration(declaration)
              .outputType(R.I64)
              .aggregationPhase(phase)
              .invocation(Expression.AggregationInvocation.ALL)
              .addArguments(sb.fieldReference(input, 0));
      if (overflow != null) {
        function.addOptions(FunctionOption.builder().name("overflow").addValues(overflow).build());
      }
      return io.substrait.relation.Aggregate.Measure.builder().function(function.build()).build();
    }

    private ResolvedAggregateBinding boundBinding(AggregateCall call) {
      return AggregateFunctions.boundBinding(call.getAggregation())
          .orElseThrow(() -> new AssertionError("expected a bound aggregate function: " + call));
    }

    private Optional<String> enumArgument(ResolvedAggregateBinding binding) {
      return binding.function().arguments().stream()
          .filter(argument -> argument.kind() == ResolvedArgument.Kind.ENUM)
          .findFirst()
          .flatMap(ResolvedArgument::enumValue);
    }

    private io.substrait.relation.Aggregate.Measure stdDev(Rel input, String distribution) {
      SimpleExtension.AggregateFunctionVariant declaration =
          extensions.getAggregateFunction(
              SimpleExtension.FunctionAnchor.of(
                  DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "std_dev:req_fp32"));
      return io.substrait.relation.Aggregate.Measure.builder()
          .function(
              AggregateFunctionInvocation.builder()
                  .declaration(declaration)
                  .arguments(
                      List.<FunctionArg>of(EnumArg.of(distribution), sb.fieldReference(input, 0)))
                  // The declaration returns fp32?; declaring it required diverges from Calcite's
                  // inference, which is what makes the binding travel on a wrapper.
                  .outputType(R.FP32)
                  .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                  .invocation(Expression.AggregationInvocation.ALL)
                  .build())
          .build();
    }

    @Test
    void differentDeclaredTypesOnDuplicateMeasuresArePreserved() {
      Rel input = sb.namedScan(List.of("example"), List.of("a", "g"), List.of(R.I32, R.STRING));
      Rel aggregate =
          sb.aggregate(
              i -> sb.grouping(i, 1),
              i ->
                  List.of(
                      withOutputType(sb.sum(i, 0), R.I64), withOutputType(sb.sum(i, 0), R.FP64)),
              input);

      RelNode relNode = substraitToCalcite.convert(aggregate);

      // The two SUM(a) measures are identical apart from their declared output type, and
      // AggregateCall equality ignores that type. They survive as distinct columns because the
      // converter opts such an aggregate out of RelBuilder's aggregate-call deduplication.
      assertRowMatch(relNode.getRowType(), R.STRING, R.I64, R.FP64);
      assertEquals(2, ((org.apache.calcite.rel.core.Aggregate) relNode).getAggCallList().size());
    }

    @Test
    void declaredTypeSurvivesAggregateRollupRule() {
      Rel input = sb.namedScan(List.of("example"), List.of("a", "g"), List.of(R.I32, R.STRING));
      Rel filtered = sb.filter(i -> sb.equal(sb.fieldReference(i, 0), sb.i32(1)), input);
      Rel aggregate =
          sb.aggregate(
              i -> sb.grouping(i, 1), i -> List.of(withOutputType(sb.sum(i, 0), R.I64)), filtered);

      RelNode converted = substraitToCalcite.convert(aggregate);
      RelNode optimized = optimize(converted, CoreRules.AGGREGATE_FILTER_TRANSPOSE);

      // The filter references the non-grouped column, so the rule would roll the aggregate up. The
      // wrapper opts out of rollup, so the declared type is left intact instead of being rederived
      // as a nullable SUM on the rolled-up top aggregate.
      assertRowMatch(optimized.getRowType(), R.STRING, R.I64);
    }

    @Test
    void declaredTypeSurvivesAggregateSplitRule() {
      Rel left = sb.namedScan(List.of("left"), List.of("a", "k"), List.of(R.I32, R.I32));
      Rel right = sb.namedScan(List.of("right"), List.of("k"), List.of(R.I32));
      Rel joined =
          sb.innerJoin(
              ji -> sb.equal(sb.fieldReference(ji, 1), sb.fieldReference(ji, 2)), left, right);
      Rel aggregate =
          sb.aggregate(
              i -> sb.grouping(), i -> List.of(withOutputType(sb.sum(i, 0), R.I64)), joined);

      RelNode converted = substraitToCalcite.convert(aggregate);
      RelNode optimized = optimize(converted, CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED);

      // The wrapper opts out of splitting, so the rule leaves the declared type intact instead of
      // rederiving a nullable SUM on the split top aggregate.
      assertRowMatch(optimized.getRowType(), R.I64);
    }

    @Test
    void strictValidationRejectsNonSpecOutputType() {
      // sum(i32) derives i64? from its declaration; a plan declaring i32? is not spec-compliant.
      Rel aggregate =
          sb.aggregate(
              input -> sb.grouping(input, 2),
              input -> List.of(withOutputType(sb.sum(input, 0), N.I32)),
              commonTable);
      SubstraitToCalcite strict =
          new SubstraitToCalcite(
              converterProvider,
              null,
              new AggregateConversion(
                  AggregateConversion.OutputTypeSource.PLAN_OUTPUT,
                  AggregateConversion.FunctionBindingValidation.EXTENSION_DECLARATION));
      assertThrows(InvalidFunctionBindingException.class, () -> strict.convert(aggregate));
    }

    @Test
    void strictValidationAcceptsAndPreservesSpecOutputType() {
      // sum(i32) -> i64? is spec-compliant; STRICT accepts it and PLAN_OUTPUT preserves it.
      Rel aggregate =
          sb.aggregate(
              input -> sb.grouping(input, 2),
              input -> List.of(withOutputType(sb.sum(input, 0), N.I64)),
              commonTable);
      SubstraitToCalcite strict =
          new SubstraitToCalcite(
              converterProvider,
              null,
              new AggregateConversion(
                  AggregateConversion.OutputTypeSource.PLAN_OUTPUT,
                  AggregateConversion.FunctionBindingValidation.EXTENSION_DECLARATION));
      RelNode relNode = strict.convert(aggregate);
      assertRowMatch(relNode.getRowType(), N.STRING, N.I64);
    }

    @Test
    void calciteInferenceModeIgnoresPlanType() {
      // With CALCITE_INFERENCE the plan's declared type is ignored and Calcite's inference wins.
      Rel aggregate =
          sb.aggregate(
              input -> sb.grouping(input, 2),
              input -> List.of(withOutputType(sb.sum(input, 0), N.I64)),
              commonTable);
      SubstraitToCalcite calciteInference =
          new SubstraitToCalcite(
              converterProvider,
              null,
              new AggregateConversion(
                  AggregateConversion.OutputTypeSource.CALCITE_INFERENCE,
                  AggregateConversion.FunctionBindingValidation.NONE));

      RelNode relNode = calciteInference.convert(aggregate);

      // Calcite's SUM infers nullable i32 for sum(i32), not the plan's i64, and no wrapper is
      // added.
      assertRowMatch(relNode.getRowType(), N.STRING, N.I32);
      org.apache.calcite.rel.core.Aggregate calciteAgg =
          (org.apache.calcite.rel.core.Aggregate) relNode;
      assertEquals(
          Optional.empty(),
          AggregateFunctions.boundBinding(calciteAgg.getAggCallList().get(0).getAggregation()));
    }

    private RelNode optimize(RelNode relNode, RelOptRule rule) {
      HepProgram program = new HepProgramBuilder().addRuleInstance(rule).build();
      HepPlanner planner = new HepPlanner(program);
      planner.setRoot(relNode);
      return planner.findBestExp();
    }

    private io.substrait.relation.Aggregate.Measure withOutputType(
        io.substrait.relation.Aggregate.Measure measure, Type outputType) {
      AggregateFunctionInvocation function =
          AggregateFunctionInvocation.builder()
              .from(measure.getFunction())
              .outputType(outputType)
              .build();
      return io.substrait.relation.Aggregate.Measure.builder()
          .from(measure)
          .function(function)
          .build();
    }
  }

  @Nested
  class Cross {
    @Test
    void direct() {
      Plan.Root root = sb.root(sb.cross(commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableTypeTwice);
    }

    @Test
    void emit() {
      Plan.Root root = sb.root(sb.cross(commonTable, commonTable, sb.remap(0, 1, 4, 6)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, R.FP32, R.I32, N.STRING);
    }
  }

  @Nested
  class Fetch {
    @Test
    void direct() {
      Plan.Root root = sb.root(sb.fetch(20, 40, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    void emit() {
      Plan.Root root = sb.root(sb.fetch(20, 40, sb.remap(0, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }

  @Nested
  class Filter {
    @Test
    void direct() {
      Plan.Root root = sb.root(sb.filter(input -> sb.bool(true), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    void emit() {
      Plan.Root root = sb.root(sb.filter(input -> sb.bool(true), sb.remap(0, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }

  @Nested
  class Join {
    @Test
    void direct() {
      Plan.Root root = sb.root(sb.innerJoin(input -> sb.bool(true), commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableTypeTwice);
    }

    @Test
    void emit() {
      Plan.Root root =
          sb.root(sb.innerJoin(input -> sb.bool(true), sb.remap(0, 6), commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }

    @Test
    void leftJoin() {
      final List<Type> joinTableType = List.of(R.STRING, R.FP64, R.BINARY);
      final Rel joinTable = sb.namedScan(List.of("join"), List.of("a", "b", "c"), joinTableType);

      Plan.Root root =
          sb.root(
              sb.project(
                  r -> sb.fieldReferences(r, 0, 1, 3),
                  sb.remap(6, 7, 8),
                  sb.join(ji -> sb.bool(true), JoinType.LEFT, joinTable, joinTable)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.STRING, R.FP64, N.STRING);
    }

    @Test
    void rightJoin() {
      final List<Type> joinTableType = List.of(R.STRING, R.FP64, R.BINARY);
      final Rel joinTable = sb.namedScan(List.of("join"), List.of("a", "b", "c"), joinTableType);

      Plan.Root root =
          sb.root(
              sb.project(
                  r -> sb.fieldReferences(r, 0, 1, 3),
                  sb.remap(6, 7, 8),
                  sb.join(ji -> sb.bool(true), JoinType.RIGHT, joinTable, joinTable)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), N.STRING, N.FP64, R.STRING);
    }

    @Test
    void outerJoin() {
      final List<Type> joinTableType = List.of(R.STRING, R.FP64, R.BINARY);
      final Rel joinTable = sb.namedScan(List.of("join"), List.of("a", "b", "c"), joinTableType);

      Plan.Root root =
          sb.root(
              sb.project(
                  r -> sb.fieldReferences(r, 0, 1, 3),
                  sb.remap(6, 7, 8),
                  sb.join(ji -> sb.bool(true), JoinType.OUTER, joinTable, joinTable)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), N.STRING, N.FP64, N.STRING);
    }
  }

  @Nested
  class NamedScan {
    @Test
    void direct() {
      Plan.Root root =
          sb.root(sb.namedScan(List.of("example"), List.of("a", "b"), List.of(R.I32, R.FP32)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, R.FP32);
    }

    @Test
    void emit() {
      Plan.Root root =
          sb.root(
              sb.namedScan(
                  List.of("example"), List.of("a", "b"), List.of(R.I32, R.FP32), sb.remap(1)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.FP32);
    }
  }

  @Nested
  class Project {
    @Test
    void direct() {
      Plan.Root root =
          sb.root(sb.project(input -> sb.fieldReferences(input, 1, 0, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(
          relNode.getRowType(), R.I32, R.FP32, N.STRING, N.BOOLEAN, R.FP32, R.I32, N.STRING);
    }

    @Test
    void emit() {
      Plan.Root root =
          sb.root(
              sb.project(
                  input -> sb.fieldReferences(input, 1, 0, 2), sb.remap(0, 2, 4, 6), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING, R.FP32, N.STRING);
    }
  }

  @Nested
  class Set {
    @Test
    void direct() {
      Plan.Root root = sb.root(sb.set(SetOp.UNION_ALL, commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    void emit() {
      Plan.Root root = sb.root(sb.set(SetOp.UNION_ALL, sb.remap(0, 2), commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }

    // MINUS_MULTISET and INTERSECTION_PRIMARY have no equivalent Calcite relation, so converting
    // them to Calcite is unsupported.
    @Test
    void minusMultisetUnsupported() {
      Plan.Root root = sb.root(sb.set(SetOp.MINUS_MULTISET, commonTable, commonTable));

      assertThrows(
          UnsupportedOperationException.class, () -> substraitToCalcite.convert(root.getInput()));
    }

    @Test
    void intersectionPrimaryUnsupported() {
      Plan.Root root = sb.root(sb.set(SetOp.INTERSECTION_PRIMARY, commonTable, commonTable));

      assertThrows(
          UnsupportedOperationException.class, () -> substraitToCalcite.convert(root.getInput()));
    }
  }

  @Nested
  class Sort {
    @Test
    void direct() {
      Plan.Root root = sb.root(sb.sort(input -> sb.sortFields(input, 0, 1, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    void emit() {
      Plan.Root root =
          sb.root(sb.sort(input -> sb.sortFields(input, 0, 1, 2), sb.remap(0, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }
}
