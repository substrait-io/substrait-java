package io.substrait.dsl;

import io.substrait.relation.ImmutableMatchRecognize;
import io.substrait.relation.MatchRecognize;

public class PatternBuilder {

  public final MatchRecognize.Pattern.Quantifier ONCE =
      ImmutableMatchRecognize.Quantifier.builder()
          .matchingStrategy(MatchRecognize.MatchingStrategy.GREEDY)
          .min(1)
          .max(1)
          .build();

  public final MatchRecognize.Pattern.Quantifier ZERO_OR_MORE =
      ImmutableMatchRecognize.Quantifier.builder()
          .min(0)
          .matchingStrategy(MatchRecognize.MatchingStrategy.GREEDY)
          .build();

  public final MatchRecognize.Pattern.Quantifier ZERO_OR_ONE =
      ImmutableMatchRecognize.Quantifier.builder()
          .min(0)
          .max(1)
          .matchingStrategy(MatchRecognize.MatchingStrategy.GREEDY)
          .build();

  public final MatchRecognize.Pattern.Quantifier ONE_OR_MORE =
      ImmutableMatchRecognize.Quantifier.builder()
          .min(1)
          .matchingStrategy(MatchRecognize.MatchingStrategy.GREEDY)
          .build();

  public io.substrait.relation.MatchRecognize.Pattern.PatternTerm leaf(
      MatchRecognize.Pattern.Quantifier quantifier, String identifier) {
    return ImmutableMatchRecognize.Leaf.builder()
        .patternIdentifier(io.substrait.relation.MatchRecognize.PatternIdentifier.of(identifier))
        .quantifier(quantifier)
        .build();
  }

  public io.substrait.relation.MatchRecognize.Pattern.PatternTerm concatenate(
      MatchRecognize.Pattern.Quantifier quantifier,
      io.substrait.relation.MatchRecognize.Pattern.PatternTerm... components) {
    return ImmutableMatchRecognize.Concatenation.builder()
        .addComponents(components)
        .quantifier(quantifier)
        .build();
  }

  public io.substrait.relation.MatchRecognize.Pattern.PatternTerm alternation(
      MatchRecognize.Pattern.Quantifier quantifier,
      io.substrait.relation.MatchRecognize.Pattern.PatternTerm... components) {
    return ImmutableMatchRecognize.Alternation.builder()
        .addComponents(components)
        .quantifier(quantifier)
        .build();
  }
}
