package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.proto.MatchRecognizeRel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
@Value.Enclosing
public abstract class MatchRecognize extends SingleInputRel implements HasExtension {

  public abstract List<Expression> getPartitionExpressions();

  public abstract List<Expression.SortField> getSortExpressions();

  public abstract List<Measure> getMeasures();

  public abstract RowsPerMatch getRowsPerMatch();

  public abstract AfterMatchSkip getAfterMatchSkip();

  public abstract Pattern getPattern();

  public abstract List<PatternDefinition> getPatternDefinitions();

  @Override
  protected Type.Struct deriveRecordType() {
    Type.Struct inputType = getInput().getRecordType();

    // https://docs.oracle.com/en/database/oracle/oracle-database/23/dwhsg/sql-pattern-matching-data-warehouses.html#GUID-CAE4A0B5-41D3-4F4B-BEE7-5B7953CDFBB5
    if (getRowsPerMatch() == RowsPerMatch.ROWS_PER_MATCH_ONE) {
      return TypeCreator.of(inputType.nullable())
          .struct(
              Stream.concat(
                  getPartitionExpressions().stream().map(Expression::getType),
                  getMeasures().stream().map(m -> m.getMeasureExpr().getType())));
    } else {
      throw new RuntimeException("CANNOT DERIVE RECORD TYPE FOR ALL ROWS PER MATCH VARIANTS YET");
    }
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableMatchRecognize.Builder builder() {
    return ImmutableMatchRecognize.builder();
  }

  @Value.Immutable
  public abstract static class Measure {
    public abstract FrameSemantics getFrameSemantics();

    public abstract Expression getMeasureExpr();

    public static ImmutableMatchRecognize.Measure.Builder builder() {
      return ImmutableMatchRecognize.Measure.builder();
    }

    public enum FrameSemantics {
      UNSPECIFIED(MatchRecognizeRel.Measure.FrameSemantics.FRAME_SEMANTICS_UNSPECIFIED),
      RUNNING(MatchRecognizeRel.Measure.FrameSemantics.FRAME_SEMANTICS_RUNNING),
      FINAL(MatchRecognizeRel.Measure.FrameSemantics.FRAME_SEMANTICS_FINAL);

      private final MatchRecognizeRel.Measure.FrameSemantics proto;

      FrameSemantics(io.substrait.proto.MatchRecognizeRel.Measure.FrameSemantics proto) {
        this.proto = proto;
      }

      public io.substrait.proto.MatchRecognizeRel.Measure.FrameSemantics toProto() {
        return this.proto;
      }

      public static FrameSemantics fromProto(MatchRecognizeRel.Measure.FrameSemantics proto) {
        for (var v : values()) {
          if (v.proto == proto) {
            return v;
          }
        }
        throw new IllegalArgumentException("Unknown type: " + proto);
      }
    }
  }

  public enum RowsPerMatch {
    ROWS_PER_MATCH_UNSPECIFIED(MatchRecognizeRel.RowsPerMatch.ROWS_PER_MATCH_UNSPECIFIED),
    ROWS_PER_MATCH_ONE(MatchRecognizeRel.RowsPerMatch.ROWS_PER_MATCH_ONE),
    ROWS_PER_MATCH_ALL(MatchRecognizeRel.RowsPerMatch.ROWS_PER_MATCH_ALL),
    ROWS_PER_MATCH_ALL_SHOW_EMPTY_MATCHES(
        MatchRecognizeRel.RowsPerMatch.ROWS_PER_MATCH_ALL_SHOW_EMPTY_MATCHES),
    ROWS_PER_MATCH_ALL_OMIT_EMPTY_MATCHES(
        MatchRecognizeRel.RowsPerMatch.ROWS_PER_MATCH_ALL_OMIT_EMPTY_MATCHES),
    ROWS_PER_MATCH_ALL_WITH_UNMATCHED_ROWS(
        MatchRecognizeRel.RowsPerMatch.ROWS_PER_MATCH_ALL_WITH_UNMATCHED_ROWS);

    private final MatchRecognizeRel.RowsPerMatch proto;

    RowsPerMatch(MatchRecognizeRel.RowsPerMatch proto) {
      this.proto = proto;
    }

    public MatchRecognizeRel.RowsPerMatch toProto() {
      return this.proto;
    }

    public static RowsPerMatch fromProto(MatchRecognizeRel.RowsPerMatch proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  @Value.Immutable
  public abstract static class AfterMatchSkip {

    public static final AfterMatchSkip PAST_LAST_ROW =
        ImmutableMatchRecognize.AfterMatchSkip.builder()
            .afterMatch(AfterMatch.AFTER_MATCH_SKIP_PAST_LAST_ROW)
            .build();
    public static final AfterMatchSkip TO_NEXT_ROW =
        ImmutableMatchRecognize.AfterMatchSkip.builder()
            .afterMatch(AfterMatch.AFTER_MATCH_SKIP_TO_NEXT_ROW)
            .build();

    public abstract AfterMatch getAfterMatch();

    public abstract Optional<PatternIdentifier> getSkipToVariable();

    public MatchRecognizeRel.AfterMatchSkip toProto() {
      return MatchRecognizeRel.AfterMatchSkip.newBuilder()
          .setOption(getAfterMatch().toProto())
          .build();
    }
  }

  public enum AfterMatch {
    AFTER_MATCH_UNSPECIFIED(MatchRecognizeRel.AfterMatchSkip.AfterMatch.AFTER_MATCH_UNSPECIFIED),
    AFTER_MATCH_SKIP_PAST_LAST_ROW(
        MatchRecognizeRel.AfterMatchSkip.AfterMatch.AFTER_MATCH_SKIP_PAST_LAST_ROW),
    AFTER_MATCH_SKIP_TO_NEXT_ROW(
        MatchRecognizeRel.AfterMatchSkip.AfterMatch.AFTER_MATCH_SKIP_TO_NEXT_ROW),
    AFTER_MATCH_SKIP_TO_FIRST(
        MatchRecognizeRel.AfterMatchSkip.AfterMatch.AFTER_MATCH_SKIP_TO_FIRST),
    AFTER_MATCH_SKIP_TO_LAST(MatchRecognizeRel.AfterMatchSkip.AfterMatch.AFTER_MATCH_SKIP_TO_LAST);

    private final MatchRecognizeRel.AfterMatchSkip.AfterMatch proto;

    AfterMatch(MatchRecognizeRel.AfterMatchSkip.AfterMatch proto) {
      this.proto = proto;
    }

    public MatchRecognizeRel.AfterMatchSkip.AfterMatch toProto() {
      return proto;
    }

    public static AfterMatch fromProto(MatchRecognizeRel.AfterMatchSkip.AfterMatch proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  @Value.Immutable
  public abstract static class Pattern {
    public abstract boolean getStartAnchor();

    public abstract boolean getEndAnchor();

    public abstract io.substrait.relation.MatchRecognize.Pattern.PatternTerm getRoot();

    public static Pattern of(boolean startAnchor, boolean endAnchor, PatternTerm root) {
      return ImmutableMatchRecognize.Pattern.builder()
          .startAnchor(startAnchor)
          .endAnchor(endAnchor)
          .root(root)
          .build();
    }

    public MatchRecognizeRel.Pattern toProto() {
      return MatchRecognizeRel.Pattern.newBuilder()
          .setStartAnchor(getStartAnchor())
          .setEndAnchor(getEndAnchor())
          .setRoot(getRoot().toProto())
          .build();
    }

    public static final Quantifier ONCE =
        ImmutableMatchRecognize.Quantifier.builder()
            .matchingStrategy(MatchingStrategy.GREEDY)
            .min(1)
            .max(1)
            .build();

    public static final Quantifier ZERO_OR_MORE =
        ImmutableMatchRecognize.Quantifier.builder()
            .min(0)
            .matchingStrategy(MatchingStrategy.GREEDY)
            .build();

    public static final Quantifier ZERO_OR_ONE =
        ImmutableMatchRecognize.Quantifier.builder()
            .min(0)
            .max(1)
            .matchingStrategy(MatchingStrategy.GREEDY)
            .build();

    @Value.Immutable
    public abstract static class Quantifier {

      public abstract MatchingStrategy getMatchingStrategy();

      public abstract Optional<Integer> getMin();

      public abstract Optional<Integer> getMax();

      public static ImmutableMatchRecognize.Quantifier.Builder builder() {
        return ImmutableMatchRecognize.Quantifier.builder();
      }

      public MatchRecognizeRel.Pattern.Quantifier toProto() {
        var builder =
            MatchRecognizeRel.Pattern.Quantifier.newBuilder()
                .setStrategy(getMatchingStrategy().toProto());
        getMin().ifPresent(builder::setMin);
        getMax().ifPresent(builder::setMax);
        return builder.build();
      }

      public static Quantifier fromProto(MatchRecognizeRel.Pattern.Quantifier proto) {
        var builder =
            ImmutableMatchRecognize.Quantifier.builder()
                .matchingStrategy(MatchingStrategy.fromProto(proto.getStrategy()));
        if (proto.hasMin()) {
          builder.min(proto.getMin());
        }
        if (proto.hasMax()) {
          builder.max(proto.getMax());
        }
        return builder.build();
      }
    }

    public interface PatternTerm {
      Quantifier getQuantifier();

      io.substrait.proto.MatchRecognizeRel.Pattern.PatternTerm toProto();
    }

    @Value.Immutable
    public abstract static class Leaf
        implements io.substrait.relation.MatchRecognize.Pattern.PatternTerm {

      public abstract PatternIdentifier getPatternIdentifier();

      public static Leaf of(String identifier, Quantifier quantifier) {
        return ImmutableMatchRecognize.Leaf.builder()
            .patternIdentifier(PatternIdentifier.of(identifier))
            .quantifier(quantifier)
            .build();
      }

      @Override
      public MatchRecognizeRel.Pattern.PatternTerm toProto() {
        return MatchRecognizeRel.Pattern.PatternTerm.newBuilder()
            .setLeaf(getPatternIdentifier().toProto())
            .setQuantifier(getQuantifier().toProto())
            .build();
      }
    }

    @Value.Immutable
    public abstract static class Concatenation
        implements io.substrait.relation.MatchRecognize.Pattern.PatternTerm {
      public abstract List<io.substrait.relation.MatchRecognize.Pattern.PatternTerm>
          getComponents();

      @Override
      public io.substrait.proto.MatchRecognizeRel.Pattern.PatternTerm toProto() {
        List<io.substrait.proto.MatchRecognizeRel.Pattern.PatternTerm> terms =
            getComponents().stream()
                .map(io.substrait.relation.MatchRecognize.Pattern.PatternTerm::toProto)
                .collect(java.util.stream.Collectors.toList());
        return MatchRecognizeRel.Pattern.PatternTerm.newBuilder()
            .setQuantifier(getQuantifier().toProto())
            .setGroup(
                io.substrait.proto.MatchRecognizeRel.Pattern.PatternGroup.newBuilder()
                    .setGrouping(
                        io.substrait.proto.MatchRecognizeRel.Pattern.PatternGrouping
                            .PATTERN_GROUPING_CONCATENATION)
                    .addAllTerms(terms)
                    .build())
            .build();
      }

      public static ImmutableMatchRecognize.Concatenation.Builder builder() {
        return ImmutableMatchRecognize.Concatenation.builder();
      }
    }

    @Value.Immutable
    public abstract static class Alternation
        implements io.substrait.relation.MatchRecognize.Pattern.PatternTerm {
      public abstract List<io.substrait.relation.MatchRecognize.Pattern.PatternTerm>
          getComponents();

      @Override
      public io.substrait.proto.MatchRecognizeRel.Pattern.PatternTerm toProto() {
        List<io.substrait.proto.MatchRecognizeRel.Pattern.PatternTerm> terms =
            getComponents().stream()
                .map(io.substrait.relation.MatchRecognize.Pattern.PatternTerm::toProto)
                .collect(java.util.stream.Collectors.toList());
        return MatchRecognizeRel.Pattern.PatternTerm.newBuilder()
            .setQuantifier(getQuantifier().toProto())
            .setGroup(
                io.substrait.proto.MatchRecognizeRel.Pattern.PatternGroup.newBuilder()
                    .setGrouping(
                        MatchRecognizeRel.Pattern.PatternGrouping.PATTERN_GROUPING_ALTERNATION)
                    .addAllTerms(terms)
                    .build())
            .build();
      }

      public static ImmutableMatchRecognize.Alternation.Builder builder() {
        return ImmutableMatchRecognize.Alternation.builder();
      }
    }
  }

  @Value.Immutable
  public abstract static class PatternDefinition {
    public abstract PatternIdentifier getPatternIdentifier();

    public abstract Expression getPredicate();

    public static ImmutableMatchRecognize.PatternDefinition.Builder builder() {
      return ImmutableMatchRecognize.PatternDefinition.builder();
    }
  }

  @Value.Immutable
  public abstract static class PatternIdentifier {
    public abstract String getIdentifier();

    public static io.substrait.relation.MatchRecognize.PatternIdentifier of(String identifier) {
      return ImmutableMatchRecognize.PatternIdentifier.builder().identifier(identifier).build();
    }

    public io.substrait.proto.PatternIdentifier toProto() {
      return io.substrait.proto.PatternIdentifier.newBuilder().setId(getIdentifier()).build();
    }
  }

  public enum MatchingStrategy {
    UNSPECIFIED(
        MatchRecognizeRel.Pattern.Quantifier.MatchingStrategy.MATCHING_STRATEGY_UNSPECIFIED),
    GREEDY(MatchRecognizeRel.Pattern.Quantifier.MatchingStrategy.MATCHING_STRATEGY_GREEDY),
    RELUCTANT(
        io.substrait.proto.MatchRecognizeRel.Pattern.Quantifier.MatchingStrategy
            .MATCHING_STRATEGY_RELUCTANT);

    private final io.substrait.proto.MatchRecognizeRel.Pattern.Quantifier.MatchingStrategy proto;

    MatchingStrategy(
        io.substrait.proto.MatchRecognizeRel.Pattern.Quantifier.MatchingStrategy proto) {
      this.proto = proto;
    }

    public io.substrait.proto.MatchRecognizeRel.Pattern.Quantifier.MatchingStrategy toProto() {
      return proto;
    }

    public static MatchingStrategy fromProto(
        MatchRecognizeRel.Pattern.Quantifier.MatchingStrategy proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }
}
