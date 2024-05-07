package io.substrait.isthmus.cli;

import com.google.protobuf.util.JsonFormat;
import io.substrait.dsl.PatternBuilder;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.ImmutableMatchRecognize;
import io.substrait.relation.MatchRecognize;
import io.substrait.relation.VirtualTableScan;
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
    patternConcatenation();
  }

  static void patternConcatenation() throws IOException {
    // Trino: TestRowPatternMatching

    SubstraitBuilder b = new SubstraitBuilder(extensions);
    PatternBuilder p = new PatternBuilder();
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
    List<ImmutableMatchRecognize.PatternDefinition> patternDefinition =
        List.of(
            MatchRecognize.PatternDefinition.builder()
                .patternIdentifier(MatchRecognize.PatternIdentifier.of("B"))
                .predicate(b.lt(b.patternRef(input, "B", 1), b.prev(input, "B", 1)))
                .build(),
            MatchRecognize.PatternDefinition.builder()
                .patternIdentifier(MatchRecognize.PatternIdentifier.of("B"))
                .predicate(b.equal(b.patternRef(input, "C", 1), b.prev(input, "C", 1)))
                .build());

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
}
