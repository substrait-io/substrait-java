package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.extension.AdvancedExtension;
import io.substrait.relation.HasExtension;
import io.substrait.relation.Project;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.TypeCreator;
import io.substrait.utils.RelSamples;
import io.substrait.utils.StringHolder;
import io.substrait.utils.StringHolderHandlingProtoRelConverter;
import io.substrait.utils.StringHolderHandlingRelProtoConverter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * Verify that the various extension types in {@link io.substrait.relation.Extension} roundtrip
 * correctly.
 *
 * <p>{@link #everyRelWithAnAdvancedExtensionIsCovered()} keeps the coverage exhaustive: every
 * relation type implementing {@link HasExtension} needs a sample, so a converter that reads a
 * rel-level {@code advanced_extension} back but never writes it cannot go unnoticed. Such a
 * converter discards third-party extensions on a read-modify-write roundtrip without raising
 * anything.
 */
class ExtensionRoundtripTest extends TestBase {

  final ProtoRelConverter protoRelConverter =
      new StringHolderHandlingProtoRelConverter(functionCollector, extensions);

  final Rel commonTable =
      sb.namedScan(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

  final AdvancedExtension commonExtension =
      AdvancedExtension.builder()
          .enhancement(new StringHolder("COMMON ENHANCEMENT"))
          .addOptimizations(new StringHolder("COMMON OPTIMIZATION"))
          .build();

  final AdvancedExtension relExtension =
      AdvancedExtension.builder()
          .enhancement(new StringHolder("REL ENHANCEMENT"))
          .addOptimizations(new StringHolder("REL OPTIMIZATION"))
          .build();

  final Map<Class<? extends Rel>, Rel> relSamples =
      new RelSamples(sb, extensions).withAdvancedExtensions(commonExtension, relExtension);

  @Override
  protected void verifyRoundTrip(Rel rel) {
    RelProtoConverter relProtoConverter =
        new StringHolderHandlingRelProtoConverter(functionCollector);
    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(rel, relReturned);
  }

  @TestFactory
  Stream<DynamicTest> relExtensions() {
    return relSamples.entrySet().stream()
        .map(
            sample ->
                DynamicTest.dynamicTest(
                    sample.getKey().getSimpleName(), () -> verifyRoundTrip(sample.getValue())));
  }

  @Test
  void everyRelWithAnAdvancedExtensionIsCovered() {
    List<Class<?>> relTypes =
        RelSamples.relTypes().stream()
            .filter(HasExtension.class::isAssignableFrom)
            .collect(Collectors.toList());
    // Without this the check below passes vacuously if the reflection above stops finding rels.
    assertFalse(relTypes.isEmpty(), "No HasExtension relation types found on RelVisitor");

    List<String> uncovered =
        relTypes.stream()
            .filter(relType -> !relSamples.containsKey(relType))
            .map(Class::getSimpleName)
            .sorted()
            .collect(Collectors.toList());

    assertEquals(
        Collections.emptyList(),
        uncovered,
        "Relation types implementing HasExtension without an advanced extension roundtrip sample");
  }

  @Nested
  class ExtensionThroughExpression {
    // There are some expression that can contains relations.
    // Check that custom extensions in these relations can be handled.

    Rel baseTable =
        sb.namedScan(
            Stream.of("test_table").collect(Collectors.toList()),
            Stream.of("test_column").collect(Collectors.toList()),
            Stream.of(TypeCreator.REQUIRED.I64).collect(Collectors.toList()));
    Rel relWithEnhancement =
        Project.builder()
            .from(sb.project(input -> Collections.emptyList(), baseTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();

    @Test
    void scalarSubquery() {
      Project rel =
          sb.project(
              input ->
                  Stream.of(
                          Expression.ScalarSubquery.builder()
                              .input(relWithEnhancement)
                              .type(TypeCreator.REQUIRED.I64)
                              .build())
                      .collect(Collectors.toList()),
              commonTable);

      verifyRoundTrip(rel);
    }

    @Test
    void inPredicate() {
      Project rel =
          sb.project(
              input ->
                  Stream.of(
                          Expression.InPredicate.builder()
                              .needles(Collections.emptyList())
                              .haystack(relWithEnhancement)
                              .build())
                      .collect(Collectors.toList()),
              commonTable);
      verifyRoundTrip(rel);
    }

    @Test
    void setPredicate() {
      Project rel =
          sb.project(
              input ->
                  Stream.of(
                          Expression.SetPredicate.builder()
                              .predicateOp(Expression.PredicateOp.PREDICATE_OP_EXISTS)
                              .tuples(relWithEnhancement)
                              .build())
                      .collect(Collectors.toList()),
              commonTable);
      verifyRoundTrip(rel);
    }
  }
}
