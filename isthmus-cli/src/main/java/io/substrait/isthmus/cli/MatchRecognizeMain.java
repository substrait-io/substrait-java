package io.substrait.isthmus.cli;

import com.google.protobuf.util.JsonFormat;
import io.substrait.dsl.PatternBuilder;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.MatchRecognizeRel;
import io.substrait.relation.ImmutableMatchRecognize;
import io.substrait.relation.MatchRecognize;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.List;

public class MatchRecognizeMain {

  protected static SimpleExtension.ExtensionCollection extensions = null;

  static {
    try {
      extensions = SimpleExtension.loadDefaults();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static final JsonFormat.TypeRegistry TYPE_REGISTRY =
      JsonFormat.TypeRegistry.newBuilder().add(MatchRecognizeRel.getDescriptor()).build();

  static final SubstraitBuilder b = new SubstraitBuilder(extensions);
  static final PatternBuilder p = new PatternBuilder();

  static TypeCreator R = TypeCreator.of(false);

  private static MatchRecognize.Measure measure(Expression expr) {
    return MatchRecognize.Measure.builder()
        .frameSemantics(MatchRecognize.Measure.FrameSemantics.UNSPECIFIED)
        .measureExpr(expr)
        .build();
  }

  private static MatchRecognize.Measure runningMeasure(Expression expr) {
    return MatchRecognize.Measure.builder()
        .frameSemantics(MatchRecognize.Measure.FrameSemantics.RUNNING)
        .measureExpr(expr)
        .build();
  }

  private static MatchRecognize.Measure finalMeasure(Expression expr) {
    return MatchRecognize.Measure.builder()
        .frameSemantics(MatchRecognize.Measure.FrameSemantics.FINAL)
        .measureExpr(expr)
        .build();
  }

  public static void main(String[] args) throws java.io.IOException {
    //    patternConcatenation();
    //    customersWith6OrMoreOrdersWithRisingPrices();
    aggregationsInMeasures();
  }

  static void patternConcatenation() throws IOException {
    // Trino: TestRowPatternMatching

    VirtualTableScan input =
        b.virtualTableScan(
            List.of("id", "value"),
            List.of(
                b.struct(b.i64(1), b.i64(90)),
                b.struct(b.i64(2), b.i64(80)),
                b.struct(b.i64(3), b.i64(70)),
                b.struct(b.i64(4), b.i64(70))));

    // -- QUERY
    // SELECT m.id AS row_id, m.match, m.val, m.label
    // FROM t2 MATCH_RECOGNIZE (
    //  ORDER BY id
    //  MEASURES
    //    match_number() AS match,
    //    RUNNING LAST(value) AS val,
    //    classifier() AS label
    //  ALL ROWS PER MATCH
    //  AFTER MATCH SKIP PAST LAST ROW
    //  PATTERN (A B C)
    //  DEFINE
    //    B AS B.value < PREV (B.value)
    //    C AS C.value = PREV (C.value)
    // ) AS m

    List<Expression.SortField> sortKeys =
        List.of(b.sortField(b.fieldReference(input, 0), Expression.SortDirection.ASC_NULLS_FIRST));

    List<MatchRecognize.Measure> measures =
        List.of(
            measure(b.matchNumber()), runningMeasure(b.last(input, 1)), measure(b.classifier()));

    MatchRecognize.Pattern.PatternTerm pattern =
        p.concatenate(p.ONCE, p.leaf(p.ONCE, "A"), p.leaf(p.ONCE, "B"), p.leaf(p.ONCE, "C"));
    List<MatchRecognize.PatternDefinition> patternDefinition =
        List.of(
            MatchRecognize.PatternDefinition.of(
                "B", b.lt(b.patternRef(input, "B", 1), b.prev(input, "B", 1))),
            MatchRecognize.PatternDefinition.of(
                "B", b.equal(b.patternRef(input, "C", 1), b.prev(input, "C", 1))));

    MatchRecognize matchRecognize =
        MatchRecognize.builder()
            .input(input)
            .sortExpressions(sortKeys)
            .measures(measures)
            .rowsPerMatch(MatchRecognize.RowsPerMatch.ROWS_PER_MATCH_ALL)
            .afterMatchSkip(MatchRecognize.AfterMatchSkip.PAST_LAST_ROW)
            .pattern(
                ImmutableMatchRecognize.Pattern.builder()
                    .startAnchor(false)
                    .endAnchor(false)
                    .root(pattern)
                    .build())
            .patternDefinitions(patternDefinition)
            .build();

    var plan = b.plan(b.root(matchRecognize));
    var planToProtoConverter = new PlanProtoConverter();

    var protoPlan = planToProtoConverter.toProto(plan);

    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(protoPlan));

    var protoPlanConverter = new io.substrait.plan.ProtoPlanConverter();
    var plan2 = protoPlanConverter.from(protoPlan);

    assert plan.equals(plan2);
  }

  // trino> SHOW COLUMNS FROM tiny.orders
  //     -> ;
  //    Column     |    Type     | Extra | Comment
  // ---------------+-------------+-------+---------
  //  orderkey      | bigint      |       |
  //  custkey       | bigint      |       |
  //  orderstatus   | varchar(1)  |       |
  //  totalprice    | double      |       |
  //  orderdate     | date        |       |
  //  orderpriority | varchar(15) |       |
  //  clerk         | varchar(15) |       |
  //  shippriority  | integer     |       |
  //  comment       | varchar(79) |       |
  static NamedScan ORDERS =
      b.namedScan(
          List.of("orders"),
          List.of(
              "orderkey", // 0
              "custkey", // 1
              "orderstatus", // 2
              "totalprice", // 3
              "orderdate", // 4
              "orderpriority", // 5
              "clerk", // 6
              "shippriority", // 7
              "comment"), // 8
          List.of(R.I64, R.I64, R.STRING, R.FP64, R.DATE, R.STRING, R.STRING, R.I64, R.STRING));

  static void customersWith6OrMoreOrdersWithRisingPrices() throws IOException {
    // SOURCE:
    // https://github.com/trinodb/trino/blob/f26bade5be88f5326e4bc243bff6bae27a93b2d2/testing/trino-testing/src/main/java/io/trino/testing/AbstractTestEngineOnlyQueries.java#L5137-L5159
    //    SubstraitBuilder b = new SubstraitBuilder(extensions);

    // SELECT m.custkey, m.matchno, m.lowest_price, m.highest_price
    // FROM orders MATCH_RECOGNIZE (
    //   PARTITION BY custkey
    //   ORDER BY orderdate
    //   MEASURES
    //     A.totalprice AS lowest_price,
    //     FINAL LAST(R.totalprice) AS highest_price,
    //     MATCH_NUMBER() AS matchno
    //   ONE ROW PER MATCH
    //   PATTERN (A R{5,})
    //   DEFINE R AS R.totalprice > PREV(R.totalprice)
    // ) AS m

    MatchRecognize matchRecognize =
        MatchRecognize.builder()
            .input(ORDERS)
            .addPartitionExpressions(b.fieldReference(ORDERS, 1))
            .sortExpressions(b.sortFields(ORDERS, 0))
            .addMeasures(
                // A.totalprice AS lowest_price
                measure(b.patternRef(ORDERS, "A", 3)),
                // FINAL LAST(R.totalprice) AS highest_price
                finalMeasure(b.last(ORDERS, "R", 3)),
                // MATCH_NUMBER() AS matchno
                measure(b.matchNumber()))
            .rowsPerMatch(MatchRecognize.RowsPerMatch.ROWS_PER_MATCH_ONE)
            .afterMatchSkip(MatchRecognize.AfterMatchSkip.PAST_LAST_ROW)
            .pattern(
                MatchRecognize.Pattern.of(
                    false,
                    false,
                    p.concatenate(
                        MatchRecognize.Pattern.ONCE,
                        // A
                        p.leaf(MatchRecognize.Pattern.ONCE, "A"),
                        // R{5,}
                        p.leaf(
                            MatchRecognize.Pattern.Quantifier.builder()
                                .matchingStrategy(MatchRecognize.MatchingStrategy.GREEDY)
                                .min(5)
                                .build(),
                            "R"))))
            .addPatternDefinitions(
                MatchRecognize.PatternDefinition.of(
                    "R", b.gt(b.patternRef(ORDERS, "R", 3), b.prev(ORDERS, "R", 3))))
            .build();

    Project select = b.project(input -> List.of(), b.remap(0, 3, 1, 2), matchRecognize);

    Plan plan =
        b.plan(b.root(select, List.of("custkey", "matchno", "lowest_price", "highest_price")));

    var planToProtoConverter = new PlanProtoConverter();
    var protoPlan = planToProtoConverter.toProto(plan);

    System.out.println(
        JsonFormat.printer()
            .usingTypeRegistry(TYPE_REGISTRY)
            .includingDefaultValueFields()
            .print(protoPlan));

    var protoPlanConverter = new io.substrait.plan.ProtoPlanConverter();
    var plan2 = protoPlanConverter.from(protoPlan);
    assert plan.equals(plan2);
  }

  static void aggregationsInMeasures() throws IOException {
    // SELECT even_count, even_sum, odd_count, odd_sum
    // FROM orders MATCH_RECOGNIZE (
    //   MEASURES
    //     count(EVEN.totalprice) AS even_count,
    //     sum(EVEN.totalprice) AS even_sum,
    //     count(ODD.totalprice) AS odd_count,
    //     sum(ODD.totalprice) AS odd_sum
    //   ONE ROW PER MATCH
    //   PATTERN ((EVEN | ODD)*)
    //   DEFINE EVEN AS orderkey % 2 = 0
    // )

    MatchRecognize matchRecognize =
        MatchRecognize.builder()
            .input(ORDERS)
            .addMeasures(
                // TODO / META: MATCH_RECOGNIZE allows usage of aggregates outside of aggregations
                // As an Expression, the only way to encode this is a a WindowFunction
                // However the bounds should be ignored as they are defined by the RUNNING / FINAL
                // option on the measure
                //
                // We may wish to add AggregateFunction as a valid Expression type OR have the
                // MatchRecognize measure
                // be either an expression or AggregateFunctions
                measure(
                        b.sumWindowed(
                            b.patternRef(ORDERS, "EVEN", 3),
                            Expression.WindowBoundsType.UNSPECIFIED,
                            WindowBound.UNBOUNDED,
                            WindowBound.UNBOUNDED)),
                    measure(
                        b.countWindowed(
                            b.patternRef(ORDERS, "EVEN", 3),
                            Expression.WindowBoundsType.UNSPECIFIED,
                            WindowBound.UNBOUNDED,
                            WindowBound.UNBOUNDED)),
                measure(
                        b.sumWindowed(
                            b.patternRef(ORDERS, "ODD", 3),
                            Expression.WindowBoundsType.UNSPECIFIED,
                            WindowBound.UNBOUNDED,
                            WindowBound.UNBOUNDED)),
                    measure(
                        b.countWindowed(
                            b.patternRef(ORDERS, "ODD", 3),
                            Expression.WindowBoundsType.UNSPECIFIED,
                            WindowBound.UNBOUNDED,
                            WindowBound.UNBOUNDED)))
            .rowsPerMatch(MatchRecognize.RowsPerMatch.ROWS_PER_MATCH_ONE)
            .afterMatchSkip(MatchRecognize.AfterMatchSkip.PAST_LAST_ROW)
            .pattern(
                MatchRecognize.Pattern.of(
                    false,
                    false,
                    p.alternation(
                        MatchRecognize.Pattern.ZERO_OR_MORE,
                        p.leaf(MatchRecognize.Pattern.ONCE, "EVEN"),
                        p.leaf(MatchRecognize.Pattern.ONCE, "ODD"))))
            .addPatternDefinitions(
                MatchRecognize.PatternDefinition.of(
                    "EVEN", b.equal(b.mod(b.fieldReference(ORDERS, 0), b.i64(2)), b.i64(0))))
            .build();

    Plan plan =
        b.plan(b.root(matchRecognize, List.of("even_count", "even_sum", "odd_count", "odd_sum")));

    var planToProtoConverter = new PlanProtoConverter();
    var protoPlan = planToProtoConverter.toProto(plan);

    System.out.println(
        JsonFormat.printer()
            .usingTypeRegistry(TYPE_REGISTRY)
            .includingDefaultValueFields()
            .print(protoPlan));

    var protoPlanConverter = new io.substrait.plan.ProtoPlanConverter();
    var plan2 = protoPlanConverter.from(protoPlan);
    assert plan.equals(plan2);
  }
}
