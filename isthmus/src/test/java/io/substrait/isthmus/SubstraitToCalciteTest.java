package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.plan.Plan.Root;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class SubstraitToCalciteTest extends PlanTestBase {
  final SubstraitToCalcite converter = new SubstraitToCalcite(extensions, typeFactory);

  @Test
  void testConvertRootSingleColumn() {
    final Iterable<Type> types = List.of(TypeCreator.REQUIRED.STRING);
    final Root root =
        Root.builder()
            .input(substraitBuilder.namedScan(List.of("stores"), List.of("s"), types))
            .addNames("store")
            .build();

    final RelRoot relRoot = converter.convert(root);

    assertEquals(root.getNames(), relRoot.fields.rightList());
  }

  @Test
  void testConvertRootMultipleColumns() {
    final Iterable<Type> types = List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    final Root root =
        Root.builder()
            .input(substraitBuilder.namedScan(List.of("stores"), List.of("s_store_id", "s"), types))
            .addNames("s_store_id", "store")
            .build();

    final RelRoot relRoot = converter.convert(root);

    assertEquals(root.getNames(), relRoot.fields.rightList());
  }

  @Test
  void testConvertRootStructField() {
    final Type structType =
        TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    final Iterable<Type> types = List.of(structType);
    final Root root =
        Root.builder()
            .input(
                substraitBuilder.namedScan(
                    List.of("stores"), List.of("s", "s_store_id", "s_store_name"), types))
            .addNames("store", "store_id", "store_name")
            .build();

    assertEquals(List.of("store", "store_id", "store_name"), root.getNames());

    final RelRoot relRoot = converter.convert(root);

    // Apache Calcite's RelRoot.fields only contains the top level field names
    assertEquals(List.of("store"), relRoot.fields.rightList());

    // the sub field names are stored within RelRoot.validatedRowType
    assertEquals(List.of("store"), relRoot.validatedRowType.getFieldNames());

    final RelDataType storeFieldDataType = relRoot.validatedRowType.getFieldList().get(0).getType();
    assertEquals(List.of("store_id", "store_name"), storeFieldDataType.getFieldNames());
  }

  @Test
  void testConvertRootArrayWithStructField() {
    final Type structType =
        TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    final Type arrayType = TypeCreator.REQUIRED.list(structType);
    final Set<Type> types = Set.of(arrayType);
    final Root root =
        Root.builder()
            .input(
                substraitBuilder.namedScan(
                    List.of("stores"), List.of("s", "s_store_id", "s_store_name"), types))
            .addNames("store", "store_id", "store_name")
            .build();

    final RelRoot relRoot = converter.convert(root);

    // Apache Calcite's RelRoot.fields only contains the top level field names
    assertEquals(List.of("store"), relRoot.fields.rightList());

    // the hierarchical structure is stored within RelRoot.validatedRowType
    assertEquals(List.of("store"), relRoot.validatedRowType.getFieldNames());

    final RelDataType storeFieldDataType = relRoot.validatedRowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.ARRAY, storeFieldDataType.getSqlTypeName());

    final RelDataType arrayElementType = storeFieldDataType.getComponentType();
    assertEquals(SqlTypeName.ROW, arrayElementType.getSqlTypeName());
    assertEquals(List.of("store_id", "store_name"), arrayElementType.getFieldNames());
  }

  @Test
  void testConvertRootMapWithStructValues() {
    final Type structType =
        TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    final Type mapValueType = TypeCreator.REQUIRED.map(TypeCreator.REQUIRED.I64, structType);
    final Set<Type> types = Set.of(mapValueType);
    final Root root =
        Root.builder()
            .input(
                substraitBuilder.namedScan(
                    List.of("stores"), List.of("s", "s_store_id", "s_store_name"), types))
            .addNames("store", "store_id", "store_name")
            .build();

    final RelRoot relRoot = converter.convert(root);

    // Apache Calcite's RelRoot.fields only contains the top level field names
    assertEquals(List.of("store"), relRoot.fields.rightList());

    // the hierarchical structure is stored within RelRoot.validatedRowType
    assertEquals(List.of("store"), relRoot.validatedRowType.getFieldNames());

    final RelDataType storeFieldDataType = relRoot.validatedRowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.MAP, storeFieldDataType.getSqlTypeName());

    final RelDataType mapValueDataType = storeFieldDataType.getValueType();
    assertEquals(SqlTypeName.ROW, mapValueDataType.getSqlTypeName());
    assertEquals(List.of("store_id", "store_name"), mapValueDataType.getFieldNames());
  }

  @Test
  void testConvertRootMapWithStructKeys() {
    final Type structType =
        TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    final Type mapKeyType = TypeCreator.REQUIRED.map(structType, TypeCreator.REQUIRED.I64);
    final Set<Type> types = Set.of(mapKeyType);
    final Root root =
        Root.builder()
            .input(
                substraitBuilder.namedScan(
                    List.of("stores"), List.of("s", "s_store_id", "s_store_name"), types))
            .addNames("store", "store_id", "store_name")
            .build();

    final RelRoot relRoot = converter.convert(root);

    // Apache Calcite's RelRoot.fields only contains the top level field names
    assertEquals(List.of("store"), relRoot.fields.rightList());

    // the hierarchical structure is stored within RelRoot.validatedRowType
    assertEquals(List.of("store"), relRoot.validatedRowType.getFieldNames());

    final RelDataType storeFieldDataType = relRoot.validatedRowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.MAP, storeFieldDataType.getSqlTypeName());

    final RelDataType mapKeyDataType = storeFieldDataType.getKeyType();
    assertEquals(SqlTypeName.ROW, mapKeyDataType.getSqlTypeName());
    assertEquals(List.of("store_id", "store_name"), mapKeyDataType.getFieldNames());
  }
}
