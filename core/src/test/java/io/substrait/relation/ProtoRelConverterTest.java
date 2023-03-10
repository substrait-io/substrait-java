package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.proto.FunctionCollector;
import io.substrait.function.SimpleExtension;
import io.substrait.io.substrait.extension.AdvancedExtension;
import io.substrait.relation.utils.StringHolder;
import io.substrait.relation.utils.StringHolderHandlingProtoRelConvert;
import java.io.IOException;
import java.util.ArrayList;
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

  @Nested
  class AdvancedExtensions {

    static final StringHolder ENHANCED = new StringHolder("ENHANCED");
    static final StringHolder OPTIMIZED = new StringHolder("OPTIMIZED");

    Rel relWithExtension(AdvancedExtension advancedExtension) {
      return b.namedScan(
          new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), advancedExtension);
    }

    Rel emptyAdvancedExtension = relWithExtension(AdvancedExtension.builder().build());
    Rel advancedExtensionWithOptimization =
        relWithExtension(AdvancedExtension.builder().optimization(OPTIMIZED).build());

    Rel advancedExtensionWithEnhancement =
        relWithExtension(AdvancedExtension.builder().enhancement(ENHANCED).build());

    Rel advancedExtensionWithEnhancementAndOptimization =
        relWithExtension(
            AdvancedExtension.builder().enhancement(ENHANCED).optimization(OPTIMIZED).build());

    @Nested
    class DefaultProtoRelConverter {

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
        assertThrows(RuntimeException.class, () -> protoRelConverter.from(protoRel));
      }
    }

    @Nested
    class CustomProtoRelConverter {

      final ProtoRelConverter protoRelConverter =
          new StringHolderHandlingProtoRelConvert(functionCollector, extensions);

      void verifyRoundTrip(Rel rel) {
        io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
        Rel relReturned = protoRelConverter.from(protoRel);
        assertEquals(rel, relReturned);
      }

      @Test
      void emptyAdvancedExtension() {
        verifyRoundTrip(emptyAdvancedExtension);
      }

      @Test
      void enhancementOnlyAdvancedExtension() {
        verifyRoundTrip(advancedExtensionWithEnhancement);
      }

      @Test
      void optimizationOnlyAdvancedExtension() {
        verifyRoundTrip(advancedExtensionWithOptimization);
      }

      @Test
      void advancedExtensionWithEnhancementAndOptimization() {
        verifyRoundTrip(advancedExtensionWithEnhancementAndOptimization);
      }
    }
  }
}
