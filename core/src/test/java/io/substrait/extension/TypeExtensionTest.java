package io.substrait.extension;

import static io.substrait.type.TypeCreator.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Verifies that user specified types can:
 *
 * <ul>
 *   <li>Be read from extension files
 *   <li>Be referenced by functions in extension files
 *   <li>Roundtrip between POJO and Proto
 * </ul>
 */
class TypeExtensionTest extends TestBase {

  static final TypeCreator R = TypeCreator.of(false);

  static final String URN = "extension:test:custom_extensions";
  static final SimpleExtension.ExtensionCollection CUSTOM_EXTENSION;

  static {
    try {
      String customExtensionStr = asString("extensions/custom_extensions.yaml");
      CUSTOM_EXTENSION = SimpleExtension.load(URN, customExtensionStr);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  Type customType1 = sb.userDefinedType(URN, "customType1");
  Type customType2 = sb.userDefinedType(URN, "customType2");
  final PlanProtoConverter planProtoConverter = new PlanProtoConverter();
  final ProtoPlanConverter protoPlanConverter;

  TypeExtensionTest() {
    super(CUSTOM_EXTENSION);
    this.protoPlanConverter = new ProtoPlanConverter(extensions);
  }

  @Test
  void roundtripCustomType() {
    // CREATE TABLE example (custom_type_column custom_type1, i64_column BIGINT);
    List<String> tableName = Stream.of("example").collect(Collectors.toList());
    List<String> columnNames =
        Stream.of("custom_type_column", "i64_column").collect(Collectors.toList());
    List<Type> types = Stream.of(customType1, R.I64).collect(Collectors.toList());

    // SELECT custom_type_column, scalar1(custom_type_column), scalar2(i64_column)
    // FROM example
    Plan plan =
        sb.plan(
            sb.root(
                sb.project(
                    input ->
                        Stream.of(
                                sb.fieldReference(input, 0),
                                sb.scalarFn(
                                    URN,
                                    "scalar1:u!customType1",
                                    R.I64,
                                    sb.fieldReference(input, 0)),
                                sb.scalarFn(
                                    URN, "scalar2:i64", customType2, sb.fieldReference(input, 1)))
                            .collect(Collectors.toList()),
                    sb.namedScan(tableName, columnNames, types))));

    io.substrait.proto.Plan protoPlan = planProtoConverter.toProto(plan);
    Plan planReturned = protoPlanConverter.from(protoPlan);
    assertEquals(plan, planReturned);
  }

  @Test
  void roundtripNumberedAnyTypes() {
    List<String> tableName = Stream.of("example").collect(Collectors.toList());
    List<String> columnNames =
        Stream.of("array_i64_type_column", "array_i64_column").collect(Collectors.toList());
    List<Type> types = Stream.of(REQUIRED.list(R.I64)).collect(Collectors.toList());

    Plan plan =
        sb.plan(
            sb.root(
                sb.project(
                    input ->
                        Stream.of(
                                sb.scalarFn(
                                    URN,
                                    "array_index:list_i64",
                                    R.I64,
                                    sb.fieldReference(input, 0)))
                            .collect(Collectors.toList()),
                    sb.namedScan(tableName, columnNames, types))));
    io.substrait.proto.Plan protoPlan = planProtoConverter.toProto(plan);
    Plan planReturned = protoPlanConverter.from(protoPlan);
    assertEquals(plan, planReturned);
  }
}
