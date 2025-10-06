package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.extension.AdvancedExtension;
import io.substrait.hint.Hint;
import io.substrait.hint.Hint.ComputationType;
import io.substrait.hint.Hint.LoadedComputation;
import io.substrait.hint.Hint.RuntimeConstraint;
import io.substrait.hint.Hint.SavedComputation;
import io.substrait.hint.Hint.Stats;
import io.substrait.hint.ImmutableRuntimeConstraint;
import io.substrait.hint.ImmutableStats;
import io.substrait.utils.StringHolder;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ProtoRelConverterTest extends TestBase {

  final NamedScan commonTable =
      b.namedScan(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

  /**
   * Verify default behaviour of {@link ProtoRelConverter} in the presence of {@link
   * AdvancedExtension} data.
   */
  @Nested
  class DefaultAdvancedExtensionTests {

    final StringHolder enhanced = new StringHolder("ENHANCED");
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    Rel emptyAdvancedExtension = relWithExtension(AdvancedExtension.builder().build());
    Rel advancedExtensionWithOptimization =
        relWithExtension(AdvancedExtension.builder().addOptimizations(optimized).build());
    Rel advancedExtensionWithEnhancement =
        relWithExtension(AdvancedExtension.builder().enhancement(enhanced).build());
    Rel advancedExtensionWithEnhancementAndOptimization =
        relWithExtension(
            AdvancedExtension.builder().enhancement(enhanced).addOptimizations(optimized).build());

    Rel relWithExtension(AdvancedExtension advancedExtension) {
      return NamedScan.builder()
          .from(commonTable)
          .commonExtension(advancedExtension)
          .extension(advancedExtension)
          .build();
    }

    @Test
    void emptyAdvancedExtension() {
      Rel rel = emptyAdvancedExtension;
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
      Rel relReturned = protoRelConverter.from(protoRel);
      assertEquals(rel, relReturned);
    }

    @Test
    void enhancementOnlyAdvancedExtension() {
      Rel rel = advancedExtensionWithEnhancement;
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
      // Enhancements are not handled by the default ProtoRelConverter
      assertThrows(
          UnsupportedOperationException.class,
          () -> protoRelConverter.from(protoRel),
          "missing deserialization logic for AdvancedExtension.Enhancement");
    }

    @Test
    void optimizationOnlyAdvancedExtension() {
      Rel rel = advancedExtensionWithOptimization;
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);

      // The optimization is serialized correctly to protobuf.
      // When it is read back in, the default ProtoRelConverter throws UnsupportedOperationException
      // since it missing the logic to deserialize the optimization.
      assertThrows(
          UnsupportedOperationException.class,
          () -> protoRelConverter.from(protoRel),
          "missing deserialization logic for AdvancedExtension.Optimization");
    }

    @Test
    void advancedExtensionWithEnhancementAndOptimization() {
      Rel rel = advancedExtensionWithEnhancementAndOptimization;
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
      // Enhancements are not handled by the default ProtoRelConverter
      assertThrows(
          UnsupportedOperationException.class,
          () -> protoRelConverter.from(protoRel),
          "missing deserialization logic for AdvancedExtension.Enhancement");
    }
  }

  /**
   * Verify default behaviour of {@link ProtoRelConverter} in the presence of Detail data. Messages
   * do NOT round trip because the default ProtoRelConverter does not handle custom Detail data.
   */
  @Nested
  class DetailsTest {

    @Test
    void extensionLeaf() {
      Rel rel = ExtensionLeaf.from(new StringHolder("DETAILS")).build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
      Rel relReturned = protoRelConverter.from(protoRel);

      assertNotEquals(rel, relReturned);
    }

    @Test
    void extensionSingle() {
      Rel rel = ExtensionSingle.from(new StringHolder("DETAILS"), commonTable).build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
      Rel relReturned = protoRelConverter.from(protoRel);

      assertNotEquals(rel, relReturned);
    }

    @Test
    void extensionMulti() {
      Rel rel = ExtensionMulti.from(new StringHolder("DETAILS"), commonTable, commonTable).build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
      Rel relReturned = protoRelConverter.from(protoRel);

      assertNotEquals(rel, relReturned);
    }

    @Test
    void extensionTable() {
      Rel rel = ExtensionTable.from(new StringHolder("DETAILS")).build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
      Rel relReturned = protoRelConverter.from(protoRel);

      assertNotEquals(rel, relReturned);
    }
  }

  /** Verify that hints are correctly transmitted in proto<->pojo */
  @Nested
  class HintsTest {

    Stats createStats(boolean includeEmptyOptimization) {
      ImmutableStats.Builder builder = Stats.builder();
      builder.rowCount(42).recordSize(42);
      if (includeEmptyOptimization) {
        builder.extension(AdvancedExtension.builder().addOptimizations().build());
      }
      return builder.build();
    }

    LoadedComputation createLoadedComputation() {
      return LoadedComputation.builder()
          .computationId(1)
          .computationType(ComputationType.COMPUTATION_TYPE_UNKNOWN)
          .build();
    }

    SavedComputation createSavedComputation() {
      return SavedComputation.builder()
          .computationId(1)
          .computationType(ComputationType.COMPUTATION_TYPE_UNKNOWN)
          .build();
    }

    RuntimeConstraint createRuntimeConstraint(boolean includeEmptyOptimization) {
      ImmutableRuntimeConstraint.Builder builder = RuntimeConstraint.builder();
      if (includeEmptyOptimization) {
        builder.extension(AdvancedExtension.builder().addOptimizations().build());
      }
      return builder.build();
    }

    @Test
    void relWithCompleteHint() {
      Hint test =
          Hint.builder()
              .alias("TestHint")
              .addAllOutputNames(Arrays.asList("Hint 1", "Hint 2"))
              .stats(createStats(true))
              .addAllLoadedComputations(
                  Arrays.asList(createLoadedComputation(), createLoadedComputation()))
              .addAllSavedComputations(
                  Arrays.asList(createSavedComputation(), createSavedComputation()))
              .runtimeConstraint(createRuntimeConstraint(true))
              .build();

      Rel relWithCompleteHint = NamedScan.builder().from(commonTable).hint(test).build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(relWithCompleteHint);
      Rel relReturned = protoRelConverter.from(protoRel);
      assertEquals(relWithCompleteHint, relReturned);
    }

    @Test
    void relWithLoadedComputationHint() {
      Hint test =
          Hint.builder()
              .alias("TestHint")
              .addAllOutputNames(Arrays.asList("Hint 1", "Hint 2"))
              .stats(createStats(false))
              .addAllLoadedComputations(
                  Arrays.asList(createLoadedComputation(), createLoadedComputation()))
              .runtimeConstraint(createRuntimeConstraint(false))
              .build();

      Rel relWithLoadedComputationHint = NamedScan.builder().from(commonTable).hint(test).build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(relWithLoadedComputationHint);
      Rel relReturned = protoRelConverter.from(protoRel);
      assertEquals(relWithLoadedComputationHint, relReturned);
    }

    @Test
    void relWithSavedComputationHint() {
      Hint test =
          Hint.builder()
              .alias("TestHint")
              .addAllOutputNames(Arrays.asList("Hint 1", "Hint 2"))
              .stats(createStats(false))
              .addAllSavedComputations(
                  Arrays.asList(createSavedComputation(), createSavedComputation()))
              .runtimeConstraint(createRuntimeConstraint(false))
              .build();

      Rel relWithSavedComputationHint = NamedScan.builder().from(commonTable).hint(test).build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(relWithSavedComputationHint);
      Rel relReturned = protoRelConverter.from(protoRel);
      assertEquals(relWithSavedComputationHint, relReturned);
    }

    @Test
    void relWithMinimalHint() {
      Hint test = Hint.builder().build();
      Rel relWithMinimalHint = NamedScan.builder().from(commonTable).hint(test).build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(relWithMinimalHint);
      Rel relReturned = protoRelConverter.from(protoRel);
      assertEquals(relWithMinimalHint, relReturned);
    }
  }
}
