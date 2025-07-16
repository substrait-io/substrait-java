package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableSet;
import io.substrait.plan.Plan.Root;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.Set;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

public class SubstraitToCalciteTest extends PlanTestBase {
  final SubstraitToCalcite converter = new SubstraitToCalcite(extensions, typeFactory);

  @Test
  void testConvertRootSingleColumn() {
    Iterable<Type> types = Arrays.asList(TypeCreator.REQUIRED.STRING);
    Root root =
        Root.builder()
            .input(substraitBuilder.namedScan(Arrays.asList("stores"), Arrays.asList("s"), types))
            .addNames("store")
            .build();

    RelRoot relRoot = converter.convert(root);

    assertEquals(root.getNames(), relRoot.fields.rightList());
  }

  @Test
  void testConvertRootMultipleColumns() {
    Iterable<Type> types = Arrays.asList(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    Root root =
        Root.builder()
            .input(
                substraitBuilder.namedScan(
                    Arrays.asList("stores"), Arrays.asList("s_store_id", "s"), types))
            .addNames("s_store_id", "store")
            .build();

    RelRoot relRoot = converter.convert(root);

    assertEquals(root.getNames(), relRoot.fields.rightList());
  }

  @Test
  void testConvertRootStructField() {
    final Type structType =
        TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    Iterable<Type> types = Arrays.asList(structType);
    Root root =
        Root.builder()
            .input(
                substraitBuilder.namedScan(
                    Arrays.asList("stores"),
                    Arrays.asList("s", "s_store_id", "s_store_name"),
                    types))
            .addNames("store", "store_id", "store_name")
            .build();

    assertEquals(Arrays.asList("store", "store_id", "store_name"), root.getNames());

    RelRoot relRoot = converter.convert(root);

    // Apache Calcite's RelRoot.fields only contains the top level field names
    assertEquals(Arrays.asList("store"), relRoot.fields.rightList());

    // the sub field names are stored within RelRoot.validatedRowType
    assertEquals(Arrays.asList("store"), relRoot.validatedRowType.getFieldNames());

    RelDataType storeFieldDataType = relRoot.validatedRowType.getFieldList().get(0).getType();
    assertEquals(Arrays.asList("store_id", "store_name"), storeFieldDataType.getFieldNames());
  }

  @Test
  void testConvertRootArrayWithStructField() {
    final Type structType =
        TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    final Type arrayType = TypeCreator.REQUIRED.list(structType);
    Set<Type> types = ImmutableSet.of(arrayType);
    Root root =
        Root.builder()
            .input(
                substraitBuilder.namedScan(
                    Arrays.asList("stores"),
                    Arrays.asList("s", "s_store_id", "s_store_name"),
                    types))
            .addNames("store", "store_id", "store_name")
            .build();

    RelRoot relRoot = converter.convert(root);

    // Apache Calcite's RelRoot.fields only contains the top level field names
    assertEquals(Arrays.asList("store"), relRoot.fields.rightList());

    // the hierarchical structure is stored within RelRoot.validatedRowType
    assertEquals(Arrays.asList("store"), relRoot.validatedRowType.getFieldNames());

    RelDataType storeFieldDataType = relRoot.validatedRowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.ARRAY, storeFieldDataType.getSqlTypeName());

    final RelDataType arrayElementType = storeFieldDataType.getComponentType();
    assertEquals(SqlTypeName.ROW, arrayElementType.getSqlTypeName());
    assertEquals(Arrays.asList("store_id", "store_name"), arrayElementType.getFieldNames());
  }

  @Test
  void testConvertRootMapWithStructValues() {
    final Type structType =
        TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    final Type mapValueType = TypeCreator.REQUIRED.map(TypeCreator.REQUIRED.I64, structType);
    Set<Type> types = ImmutableSet.of(mapValueType);
    Root root =
        Root.builder()
            .input(
                substraitBuilder.namedScan(
                    Arrays.asList("stores"),
                    Arrays.asList("s", "s_store_id", "s_store_name"),
                    types))
            .addNames("store", "store_id", "store_name")
            .build();

    final RelRoot relRoot = converter.convert(root);

    // Apache Calcite's RelRoot.fields only contains the top level field names
    assertEquals(Arrays.asList("store"), relRoot.fields.rightList());

    // the hierarchical structure is stored within RelRoot.validatedRowType
    assertEquals(Arrays.asList("store"), relRoot.validatedRowType.getFieldNames());

    final RelDataType storeFieldDataType = relRoot.validatedRowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.MAP, storeFieldDataType.getSqlTypeName());

    final RelDataType mapValueDataType = storeFieldDataType.getValueType();
    assertEquals(SqlTypeName.ROW, mapValueDataType.getSqlTypeName());
    assertEquals(Arrays.asList("store_id", "store_name"), mapValueDataType.getFieldNames());
  }

  @Test
  void testConvertRootMapWithStructKeys() {
    final Type structType =
        TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    final Type mapKeyType = TypeCreator.REQUIRED.map(structType, TypeCreator.REQUIRED.I64);
    Set<Type> types = ImmutableSet.of(mapKeyType);
    Root root =
        Root.builder()
            .input(
                substraitBuilder.namedScan(
                    Arrays.asList("stores"),
                    Arrays.asList("s", "s_store_id", "s_store_name"),
                    types))
            .addNames("store", "store_id", "store_name")
            .build();

    RelRoot relRoot = converter.convert(root);

    // Apache Calcite's RelRoot.fields only contains the top level field names
    assertEquals(Arrays.asList("store"), relRoot.fields.rightList());

    // the hierarchical structure is stored within RelRoot.validatedRowType
    assertEquals(Arrays.asList("store"), relRoot.validatedRowType.getFieldNames());

    RelDataType storeFieldDataType = relRoot.validatedRowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.MAP, storeFieldDataType.getSqlTypeName());

    final RelDataType mapKeyDataType = storeFieldDataType.getKeyType();
    assertEquals(SqlTypeName.ROW, mapKeyDataType.getSqlTypeName());
    assertEquals(Arrays.asList("store_id", "store_name"), mapKeyDataType.getFieldNames());
  }
}
