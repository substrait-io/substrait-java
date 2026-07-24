package io.substrait.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.util.JsonFormat;
import io.substrait.TestBase;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/core/serialization.md}. Regions marked with {@code //
 * --8<-- [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--} snippet
 * includes.
 */
class SerializationDocTest extends TestBase {

  private final SubstraitBuilder b = new SubstraitBuilder();

  private Plan samplePlan() {
    NamedScan scan = b.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I32, R.STRING));
    return b.plan(b.root(scan, List.of("a", "b")));
  }

  @Test
  void planRoundtrip() {
    Plan plan = samplePlan();
    // --8<-- [start:plan-roundtrip]
    // POJO -> proto
    io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);

    // proto -> POJO
    Plan roundTripped = new ProtoPlanConverter().from(proto);
    // --8<-- [end:plan-roundtrip]
    assertNotNull(proto);
    assertNotNull(roundTripped);
  }

  @Test
  void customCollection() {
    SimpleExtension.ExtensionCollection myExtensions = extensions;
    // --8<-- [start:custom-collection]
    PlanProtoConverter toProto = new PlanProtoConverter(myExtensions);
    ProtoPlanConverter fromProto = new ProtoPlanConverter(myExtensions);
    // --8<-- [end:custom-collection]
    assertNotNull(toProto);
    assertNotNull(fromProto);
  }

  @Test
  void encodeToBytes() throws Exception {
    io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(samplePlan());
    // --8<-- [start:encode-bytes]
    // serialize to a byte array
    byte[] bytes = proto.toByteArray();

    // parse back from bytes
    io.substrait.proto.Plan parsed = io.substrait.proto.Plan.parseFrom(bytes);

    // then convert to the POJO model
    Plan plan = new ProtoPlanConverter().from(parsed);
    // --8<-- [end:encode-bytes]
    assertNotNull(plan);
  }

  @Test
  void encodeToJson() throws Exception {
    io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(samplePlan());
    // --8<-- [start:encode-json]
    // proto -> JSON
    String json = JsonFormat.printer().print(proto);

    // JSON -> proto
    io.substrait.proto.Plan.Builder builder = io.substrait.proto.Plan.newBuilder();
    JsonFormat.parser().merge(json, builder);
    io.substrait.proto.Plan fromJson = builder.build();
    // --8<-- [end:encode-json]
    assertNotNull(fromJson);
  }

  @Test
  void lowerLevelConverters() {
    Rel rel = b.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I32, R.STRING));
    // --8<-- [start:lower-level]
    ExtensionCollector collector = new ExtensionCollector();
    RelProtoConverter relToProto = new RelProtoConverter(collector);

    io.substrait.proto.Rel protoRel = relToProto.toProto(rel);

    ProtoRelConverter protoToRel =
        new ProtoRelConverter(collector, DefaultExtensionCatalog.DEFAULT_COLLECTION);
    io.substrait.relation.Rel back = protoToRel.from(protoRel);
    // --8<-- [end:lower-level]
    assertNotNull(back);
  }
}
