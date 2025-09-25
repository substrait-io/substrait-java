package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.SimpleExtensionURI;
import io.substrait.proto.SimpleExtensionURN;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.PlanRel;

import org.junit.jupiter.api.Test;

/**
 * Tests describing the desired URI ↔ URN migration behaviour. These are disabled until the runtime
 * support is implemented.
 */
public class UriUrnMigrationTest {

  private static final String SAMPLE_URI = "https://example.com/extensions/sample.yaml";
  private static final String SAMPLE_YAML =
      "%YAML 1.2\n"
          + "---\n"
          + "urn: extension:test:sample\n"
          + "scalar_functions:\n"
          + "  - name: add\n"
          + "    impls:\n"
          + "      - args:\n"
          + "          - value: i32\n"
          + "          - value: i32\n"
          + "        return: i32\n";

  @Test
  void uriOnlyPlanShouldHaveUrn() throws Exception {
    SimpleExtension.ExtensionCollection extensions = SimpleExtension.load(SAMPLE_URI, SAMPLE_YAML);
    io.substrait.proto.Plan protoPlan =
        io.substrait.proto.Plan.newBuilder()
            .addExtensionUrns(
                SimpleExtensionURN.newBuilder()
                    .setExtensionUrnAnchor(1)
                    .setUrn("extension:test:sample")
                    .build())
            .addExtensions(
                SimpleExtensionDeclaration.newBuilder()
                    .setExtensionFunction(
                        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                            .setFunctionAnchor(1)
                            .setName("add:i32_i32")
                            .setExtensionUrnReference(1)
                            .build())
                    .build())
            .addRelations(
                PlanRel.newBuilder()
                    .setRoot(
                        io.substrait.proto.RelRoot.newBuilder()
                            .setInput(
                                io.substrait.proto.Rel.newBuilder()
                                    .setProject(
                                        io.substrait.proto.ProjectRel.newBuilder()
                                            .setInput(
                                                io.substrait.proto.Rel.newBuilder()
                                                    .setRead(
                                                        io.substrait.proto.ReadRel.newBuilder()
                                                            .setNamedTable(
                                                                io.substrait.proto.ReadRel
                                                                    .NamedTable.newBuilder()
                                                                    .addNames("dummy")
                                                                    .build())
                                                            .setBaseSchema(
                                                                io.substrait.proto.NamedStruct
                                                                    .newBuilder()
                                                                    .addNames("col")
                                                                    .setStruct(
                                                                        io.substrait.proto.Type
                                                                            .Struct.newBuilder()
                                                                            .addTypes(
                                                                                io.substrait.proto
                                                                                    .Type
                                                                                    .newBuilder()
                                                                                    .setI32(
                                                                                        io.substrait
                                                                                            .proto
                                                                                            .Type
                                                                                            .I32
                                                                                            .newBuilder())
                                                                                    .build())
                                                                            .build())
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .addExpressions(
                                                io.substrait.proto.Expression.newBuilder()
                                                    .setScalarFunction(
                                                        io.substrait.proto.Expression.ScalarFunction
                                                            .newBuilder()
                                                            .setFunctionReference(
                                                                1) // Uses our add function
                                                            .addArguments(
                                                                io.substrait.proto.FunctionArgument
                                                                    .newBuilder()
                                                                    .setValue(
                                                                        io.substrait.proto
                                                                            .Expression.newBuilder()
                                                                            .setLiteral(
                                                                                io.substrait.proto
                                                                                    .Expression
                                                                                    .Literal
                                                                                    .newBuilder()
                                                                                    .setI32(1)
                                                                                    .build())
                                                                            .build())
                                                                    .build())
                                                            .addArguments(
                                                                io.substrait.proto.FunctionArgument
                                                                    .newBuilder()
                                                                    .setValue(
                                                                        io.substrait.proto
                                                                            .Expression.newBuilder()
                                                                            .setLiteral(
                                                                                io.substrait.proto
                                                                                    .Expression
                                                                                    .Literal
                                                                                    .newBuilder()
                                                                                    .setI32(2)
                                                                                    .build())
                                                                            .build())
                                                                    .build())
                                                            .setOutputType(
                                                                io.substrait.proto.Type.newBuilder()
                                                                    .setI32(
                                                                        io.substrait.proto.Type.I32
                                                                            .newBuilder())
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .addNames("result")
                            .build())
                    .build())
            .build();

    Plan planFromProto = new ProtoPlanConverter(extensions).from(protoPlan);

    assertTrue(planFromProto.getExtensionUris().size() > 0, "Plan should have URI");
    assertTrue(planFromProto.getExtensionUrns().size() > 0, "Plan should have URN");
  }
}
