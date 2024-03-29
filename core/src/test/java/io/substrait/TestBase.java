package io.substrait;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.TypeCreator;
import java.io.IOException;

public abstract class TestBase {

  protected static final SimpleExtension.ExtensionCollection defaultExtensionCollection;

  static {
    try {
      defaultExtensionCollection = SimpleExtension.loadDefaults();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected TypeCreator R = TypeCreator.REQUIRED;

  protected SubstraitBuilder b = new SubstraitBuilder(defaultExtensionCollection);
  protected ExtensionCollector functionCollector = new ExtensionCollector();
  protected RelProtoConverter relProtoConverter = new RelProtoConverter(functionCollector);
  protected ProtoRelConverter protoRelConverter =
      new ProtoRelConverter(functionCollector, defaultExtensionCollection);

  protected void verifyRoundTrip(Rel rel) {
    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(rel, relReturned);
  }
}
