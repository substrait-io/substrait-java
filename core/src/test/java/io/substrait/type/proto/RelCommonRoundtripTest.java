package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.hint.Hint;
import io.substrait.proto.RelCommon;
import io.substrait.relation.LateralJoin;
import io.substrait.relation.NamedScan;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.SingleInputRel;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import io.substrait.utils.RelSamples;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that every relation type carries the {@link io.substrait.proto.RelCommon} data the POJO
 * model represents — the {@link Rel#getHint() hint}, {@link Rel#getRemap() emit mapping}, {@link
 * Rel#getCommonExtension() common extension} and {@link Rel#getRelAnchor() rel anchor} — through a
 * POJO → proto → POJO round trip.
 *
 * <p>The samples are the shared {@link RelSamples}, kept exhaustive by their own test: a relation
 * added to {@link RelVisitor} without a sample fails there, so it cannot silently miss its {@code
 * RelCommon} wiring here.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RelCommonRoundtripTest extends StringHolderRoundtripTestBase {

  static final Hint HINT =
      Hint.builder()
          .extension(AdvancedExtension.builder().build())
          .alias("an_alias")
          .addAllOutputNames(Arrays.asList("name1", "name2"))
          .stats(Hint.Stats.builder().rowCount(42).recordSize(13).build())
          .runtimeConstraint(Hint.RuntimeConstraint.builder().build())
          .addLoadedComputations(
              Hint.LoadedComputation.builder()
                  .computationId(1)
                  .computationType(Hint.ComputationType.COMPUTATION_TYPE_HASHTABLE)
                  .build())
          .addSavedComputations(
              Hint.SavedComputation.builder()
                  .computationId(2)
                  .computationType(Hint.ComputationType.COMPUTATION_TYPE_BLOOM_FILTER)
                  .build())
          .build();

  static final AdvancedExtension COMMON_EXTENSION = AdvancedExtension.builder().build();

  static final int REL_ANCHOR = 11;

  final Map<Class<? extends Rel>, Rel> relSamples = new RelSamples(sb, extensions).samples();

  /** A plain relation for the hand-written {@link Rel}s below to wrap. */
  final Rel scan = relSamples.get(NamedScan.class);

  @ParameterizedTest(name = "{0}")
  @MethodSource("samples")
  void relCommonRoundtrips(String relationType, Rel rel) {
    verifyRoundTrip(withRelCommon(rel));
  }

  @Test
  void samplesCarryTheDataUnderTest() {
    // Guards the round-trip assertions above against silently asserting on empty optionals.
    for (Rel rel : relSamples.values()) {
      Rel sample = withRelCommon(rel);
      assertEquals(Optional.of(HINT), sample.getHint());
      assertEquals(Optional.of(COMMON_EXTENSION), sample.getCommonExtension());
      assertTrue(sample.getRemap().isPresent());
      assertTrue(sample.getRelAnchor().isPresent());
    }
  }

  @Test
  void applyRelCommonLeavesACustomRelAloneWhenThereIsNothingToApply() {
    // A custom, non-Immutables Rel inherits Rel's throwing withXxx defaults. applyRelCommon is a
    // documented extension point that every newXxx must route through, so it must not fail on a
    // relation whose common message carries no data.
    ApplyRelCommonConverter converter = new ApplyRelCommonConverter(functionCollector, extensions);
    Rel custom = new PassThroughRel(scan);

    assertEquals(custom, converter.applyRelCommon(custom, RelCommon.getDefaultInstance()));
    assertEquals(
        custom,
        converter.applyRelCommon(
            custom,
            RelCommon.newBuilder().setDirect(RelCommon.Direct.getDefaultInstance()).build()));
  }

  @Test
  void applyRelCommonLeavesADelegatingCustomRelAlone() {
    // PassThroughRel reports Optional.empty() from every accessor, so it cannot catch the case
    // where applyRelCommon decides a field "differs". A transparent wrapper derives its common data
    // from its input instead, and so reports data that the enclosing direct{} message does not
    // carry. applyRelCommon must leave that alone rather than try to clear it, which would destroy
    // the input's own common data (and here hits Rel's throwing withXxx defaults).
    ApplyRelCommonConverter converter = new ApplyRelCommonConverter(functionCollector, extensions);
    Rel inner = withRelCommon(scan);
    Rel custom = new DelegatingRel(inner, scan.getRecordType());

    assertTrue(custom.getHint().isPresent());
    assertTrue(custom.getCommonExtension().isPresent());
    assertTrue(custom.getRemap().isPresent());
    assertTrue(custom.getRelAnchor().isPresent());
    assertEquals(inner.getRecordType(), custom.getRecordType());

    assertEquals(custom, converter.applyRelCommon(custom, RelCommon.getDefaultInstance()));
    assertEquals(
        custom,
        converter.applyRelCommon(
            custom,
            RelCommon.newBuilder().setDirect(RelCommon.Direct.getDefaultInstance()).build()));
  }

  @Test
  void applyRelCommonRejectsACopyMethodThatReturnsAnotherRelation() {
    // A custom Rel whose withXxx returns its delegate rather than a re-wrapped copy would otherwise
    // surface as a ClassCastException at the caller's assignment, naming neither culprit.
    ApplyRelCommonConverter converter = new ApplyRelCommonConverter(functionCollector, extensions);
    Rel custom = new ForwardingRel(scan);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                converter.applyRelCommon(
                    custom,
                    RelCommon.newBuilder()
                        .setDirect(RelCommon.Direct.getDefaultInstance())
                        .setHint(RelCommon.Hint.newBuilder().setAlias("an_alias").build())
                        .build()));

    assertTrue(failure.getMessage().contains(ForwardingRel.class.getName()));
    assertTrue(failure.getMessage().contains(scan.getClass().getName()));
  }

  /** Exposes {@link ProtoRelConverter#applyRelCommon} so the test can call it directly. */
  static final class ApplyRelCommonConverter extends ProtoRelConverter {
    ApplyRelCommonConverter(
        ExtensionLookup lookup, SimpleExtension.ExtensionCollection extensions) {
      super(lookup, extensions);
    }

    @Override
    public <R extends Rel> R applyRelCommon(R rel, RelCommon relCommon) {
      return super.applyRelCommon(rel, relCommon);
    }
  }

  /** A minimal hand-written {@link Rel} that inherits {@code Rel}'s throwing {@code withXxx}. */
  static final class PassThroughRel extends SingleInputRel {
    private final Rel input;

    PassThroughRel(Rel input) {
      this.input = input;
    }

    @Override
    public Rel getInput() {
      return input;
    }

    @Override
    protected Type.Struct deriveRecordType() {
      return input.getRecordType();
    }

    @Override
    public Optional<Rel.Remap> getRemap() {
      return Optional.empty();
    }

    @Override
    public Optional<AdvancedExtension> getCommonExtension() {
      return Optional.empty();
    }

    @Override
    public Optional<Hint> getHint() {
      return Optional.empty();
    }

    @Override
    public Optional<Integer> getRelAnchor() {
      return Optional.empty();
    }

    @Override
    public <O, C extends VisitationContext, E extends Exception> O accept(
        RelVisitor<O, C, E> visitor, C context) {
      throw new UnsupportedOperationException("not visitable");
    }
  }

  /**
   * A hand-written {@link Rel} that derives all of its {@code RelCommon} data from its input, as a
   * transparent wrapper would. It inherits {@code Rel}'s throwing {@code withXxx} defaults, so any
   * attempt to clear one of those fields fails loudly.
   *
   * <p>Its derived record type is the input's type <em>before</em> the input's own emit mapping,
   * which is what makes delegating {@link #getRemap()} correct here: {@code
   * AbstractRel#getRecordType()} applies the mapping itself, so a wrapper deriving from the input's
   * already-emitted type has to hold a mapping of its own rather than inherit one.
   */
  static final class DelegatingRel extends SingleInputRel {
    private final Rel input;
    private final Type.Struct inputRecordTypeBeforeEmit;

    DelegatingRel(Rel input, Type.Struct inputRecordTypeBeforeEmit) {
      this.input = input;
      this.inputRecordTypeBeforeEmit = inputRecordTypeBeforeEmit;
    }

    @Override
    public Rel getInput() {
      return input;
    }

    @Override
    protected Type.Struct deriveRecordType() {
      return inputRecordTypeBeforeEmit;
    }

    @Override
    public Optional<Rel.Remap> getRemap() {
      return input.getRemap();
    }

    @Override
    public Optional<AdvancedExtension> getCommonExtension() {
      return input.getCommonExtension();
    }

    @Override
    public Optional<Hint> getHint() {
      return input.getHint();
    }

    @Override
    public Optional<Integer> getRelAnchor() {
      return input.getRelAnchor();
    }

    @Override
    public <O, C extends VisitationContext, E extends Exception> O accept(
        RelVisitor<O, C, E> visitor, C context) {
      throw new UnsupportedOperationException("not visitable");
    }
  }

  /**
   * A hand-written {@link Rel} whose copy methods hand back their delegate instead of a re-wrapped
   * copy of themselves — the override mistake {@code applyRelCommon}'s type guard reports.
   */
  static final class ForwardingRel extends SingleInputRel {
    private final Rel input;

    ForwardingRel(Rel input) {
      this.input = input;
    }

    @Override
    public Rel getInput() {
      return input;
    }

    @Override
    protected Type.Struct deriveRecordType() {
      return input.getRecordType();
    }

    @Override
    public Optional<Rel.Remap> getRemap() {
      return Optional.empty();
    }

    @Override
    public Optional<AdvancedExtension> getCommonExtension() {
      return Optional.empty();
    }

    @Override
    public Optional<Hint> getHint() {
      return Optional.empty();
    }

    @Override
    public Optional<Integer> getRelAnchor() {
      return Optional.empty();
    }

    @Override
    public Rel withHint(Optional<? extends Hint> hint) {
      return input.withHint(hint);
    }

    @Override
    public <O, C extends VisitationContext, E extends Exception> O accept(
        RelVisitor<O, C, E> visitor, C context) {
      throw new UnsupportedOperationException("not visitable");
    }
  }

  Stream<Arguments> samples() {
    return relSamples.entrySet().stream()
        .map(sample -> Arguments.of(sample.getKey().getSimpleName(), sample.getValue()));
  }

  /**
   * Stamps the full set of {@code RelCommon} data onto {@code rel} using the type-agnostic {@code
   * Rel.withXxx} copy methods. An existing rel anchor is kept because a {@link LateralJoin}'s right
   * input resolves its outer references against it.
   */
  static Rel withRelCommon(Rel rel) {
    Optional<Integer> relAnchor =
        rel.getRelAnchor().isPresent() ? rel.getRelAnchor() : Optional.of(REL_ANCHOR);
    return rel.withHint(Optional.of(HINT))
        .withCommonExtension(Optional.of(COMMON_EXTENSION))
        .withRemap(Optional.of(reversedRemap(rel)))
        .withRelAnchor(relAnchor);
  }

  /**
   * An emit mapping that reverses {@code rel}'s fields. Reversing rather than using the identity
   * makes the assertions sensitive to the order and count of {@code RelCommon.Emit.output_mapping},
   * not just to its presence.
   */
  static Rel.Remap reversedRemap(Rel rel) {
    List<Integer> indices = new ArrayList<>();
    for (int i = rel.getRecordType().fields().size() - 1; i >= 0; i--) {
      indices.add(i);
    }
    return Rel.Remap.of(indices);
  }
}
