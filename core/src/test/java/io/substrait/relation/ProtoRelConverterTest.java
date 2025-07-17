package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.extension.AdvancedExtension;
import io.substrait.hint.Hint;
import io.substrait.relation.utils.StringHolder;
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

    Rel relWithExtension(AdvancedExtension advancedExtension) {
      return NamedScan.builder()
          .from(commonTable)
          .commonExtension(advancedExtension)
          .extension(advancedExtension)
          .build();
    }

    Rel emptyAdvancedExtension = relWithExtension(AdvancedExtension.builder().build());
    Rel advancedExtensionWithOptimization =
        relWithExtension(AdvancedExtension.builder().addOptimizations(optimized).build());
    Rel advancedExtensionWithEnhancement =
        relWithExtension(AdvancedExtension.builder().enhancement(enhanced).build());
    Rel advancedExtensionWithEnhancementAndOptimization =
        relWithExtension(
            AdvancedExtension.builder().enhancement(enhanced).addOptimizations(optimized).build());

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
      assertThrows(RuntimeException.class, () -> protoRelConverter.from(protoRel));
    }

    @Test
    void optimizationOnlyAdvancedExtension() {
      Rel rel = advancedExtensionWithOptimization;
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
      Rel relReturned = protoRelConverter.from(protoRel);

      // The optimization is serialized correctly to protobuf.
      // When it is read back in, the default ProtoRelConverter drops it.
      // As such they are not equal anymore.
      assertNotEquals(rel, relReturned);
    }

    @Test
    void advancedExtensionWithEnhancementAndOptimization() {
      Rel rel = advancedExtensionWithEnhancementAndOptimization;
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
      // Enhancements are not handled by the default ProtoRelConverter
      assertThrows(RuntimeException.class, () -> protoRelConverter.from(protoRel));
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

    @Test
    void relWithHint() {
      Rel relWithHints =
          NamedScan.builder()
              .from(commonTable)
              .hint(Hint.builder().addOutputNames("Test hint").build())
              .build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(relWithHints);
      Rel relReturned = protoRelConverter.from(protoRel);
      assertEquals(relWithHints, relReturned);
    }

    @Test
    void relWithHints() {
      Rel relWithHints =
          NamedScan.builder()
              .from(commonTable)
              .hint(Hint.builder().addAllOutputNames(Arrays.asList("Hint 1", "Hint 2")).build())
              .build();
      io.substrait.proto.Rel protoRel = relProtoConverter.toProto(relWithHints);
      Rel relReturned = protoRelConverter.from(protoRel);
      assertEquals(relWithHints, relReturned);
    }
  }
}
