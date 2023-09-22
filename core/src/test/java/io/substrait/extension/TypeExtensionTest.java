package io.substrait.extension;

import static io.substrait.type.TypeCreator.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.InputStream;
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
public class TypeExtensionTest {

  static final TypeCreator R = TypeCreator.of(false);

  static final String NAMESPACE = "/custom_extensions";
  final SimpleExtension.ExtensionCollection extensionCollection;

  {
    InputStream inputStream =
        this.getClass().getResourceAsStream("/extensions/custom_extensions.yaml");
    extensionCollection = SimpleExtension.load(NAMESPACE, inputStream);
  }

  final SubstraitBuilder b = new SubstraitBuilder(extensionCollection);
  Type customType1 = b.userDefinedType(NAMESPACE, "customType1");
  Type customType2 = b.userDefinedType(NAMESPACE, "customType2");
  final PlanProtoConverter planProtoConverter = new PlanProtoConverter();
  final ProtoPlanConverter protoPlanConverter = new ProtoPlanConverter(extensionCollection);

  @Test
  void roundtripCustomType() {
    // CREATE TABLE example (custom_type_column custom_type1, i64_column BIGINT);
    List<String> tableName = Stream.of("example").collect(Collectors.toList());
    List<String> columnNames =
        Stream.of("custom_type_column", "i64_column").collect(Collectors.toList());
    List<io.substrait.type.Type> types = Stream.of(customType1, R.I64).collect(Collectors.toList());

    // SELECT custom_type_column, scalar1(custom_type_column), scalar2(i64_column)
    // FROM example
    Plan plan =
        b.plan(
            b.root(
                b.project(
                    input ->
                        Stream.of(
                                b.fieldReference(input, 0),
                                b.scalarFn(
                                    NAMESPACE,
                                    "scalar1:u!customType1",
                                    R.I64,
                                    b.fieldReference(input, 0)),
                                b.scalarFn(
                                    NAMESPACE,
                                    "scalar2:i64",
                                    customType2,
                                    b.fieldReference(input, 1)))
                            .collect(Collectors.toList()),
                    b.namedScan(tableName, columnNames, types))));

    var protoPlan = planProtoConverter.toProto(plan);
    var planReturned = protoPlanConverter.from(protoPlan);
    assertEquals(plan, planReturned);
  }

  @Test
  void roundtripNumberedAnyTypes() {
    List<String> tableName = Stream.of("example").collect(Collectors.toList());
    List<String> columnNames =
        Stream.of("array_i64_type_column", "array_i64_column").collect(Collectors.toList());
    List<io.substrait.type.Type> types =
        Stream.of(REQUIRED.list(R.I64)).collect(Collectors.toList());

    Plan plan =
        b.plan(
            b.root(
                b.project(
                    input ->
                        Stream.of(
                                b.scalarFn(
                                    NAMESPACE,
                                    "array_index:list_i64",
                                    R.I64,
                                    b.fieldReference(input, 0)))
                            .collect(Collectors.toList()),
                    b.namedScan(tableName, columnNames, types))));
    var protoPlan = planProtoConverter.toProto(plan);
    var planReturned = protoPlanConverter.from(protoPlan);
    assertEquals(plan, planReturned);
  }
}
