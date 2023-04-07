package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.proto.FunctionCollector;
import io.substrait.function.SimpleExtension;
import io.substrait.io.substrait.extension.AdvancedExtension;
import io.substrait.relation.utils.StringHolder;
import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ProtoRelConverterTest {

  final SimpleExtension.ExtensionCollection extensions;

  {
    try {
      extensions = SimpleExtension.loadDefaults();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  final SubstraitBuilder b = new SubstraitBuilder(extensions);
  final FunctionCollector functionCollector = new FunctionCollector();
  final RelProtoConverter relProtoConverter = new RelProtoConverter(functionCollector);
  final ProtoRelConverter protoRelConverter = new ProtoRelConverter(functionCollector, extensions);

  final NamedScan commonTable =
      b.namedScan(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

  /**
   * Verify default behaviour of {@link ProtoRelConverter} in the presence of {@link
   * AdvancedExtension} data.
   */
  @Nested
  class DefaultAdvancedExtensionTests {

    static final StringHolder ENHANCED = new StringHolder("ENHANCED");
    static final StringHolder OPTIMIZED = new StringHolder("OPTIMIZED");

    Rel relWithExtension(AdvancedExtension advancedExtension) {
      return NamedScan.builder()
          .from(commonTable)
          .commonExtension(advancedExtension)
          .extension(advancedExtension)
          .build();
    }

    Rel emptyAdvancedExtension = relWithExtension(AdvancedExtension.builder().build());
    Rel advancedExtensionWithOptimization =
        relWithExtension(AdvancedExtension.builder().optimization(OPTIMIZED).build());
    Rel advancedExtensionWithEnhancement =
        relWithExtension(AdvancedExtension.builder().enhancement(ENHANCED).build());
    Rel advancedExtensionWithEnhancementAndOptimization =
        relWithExtension(
            AdvancedExtension.builder().enhancement(ENHANCED).optimization(OPTIMIZED).build());

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
}
