package io.substrait.dsl;

import io.substrait.relation.ImmutableMatchRecognize;
import io.substrait.relation.MatchRecognize;

public class PatternBuilder {

  public MatchRecognize.Pattern.Quantifier ONCE = MatchRecognize.Pattern.ONCE;

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
