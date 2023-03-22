package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.proto.FunctionCollector;
import io.substrait.function.SimpleExtension;
import io.substrait.io.substrait.extension.AdvancedExtension;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ExtensionTable;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.utils.StringHolder;
import io.substrait.relation.utils.StringHolderHandlingProtoRelConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * Verify that the various extension types in {@link io.substrait.relation.Extension} roundtrip
 * correctly.
 */
public class ExtensionRoundtripTest {

  TypeCreator R = TypeCreator.REQUIRED;

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
  final ProtoRelConverter protoRelConverter =
      new StringHolderHandlingProtoRelConverter(functionCollector, extensions);

  final Rel commonTable =
      b.namedScan(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

  final AdvancedExtension commonExtension =
      AdvancedExtension.builder()
          .enhancement(new StringHolder("COMMON ENHANCEMENT"))
          .optimization(new StringHolder("COMMON OPTIMIZATION"))
          .build();

  final StringHolder detail = new StringHolder("DETAIL");

  final AdvancedExtension relExtension =
      AdvancedExtension.builder()
          .enhancement(new StringHolder("REL ENHANCEMENT"))
          .optimization(new StringHolder("REL OPTIMIZATION"))
          .build();

  void verifyRoundTrip(Rel rel) {
    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void virtualTable() {
    Rel rel =
        VirtualTableScan.builder()
            .addRows(Expression.StructLiteral.builder().fields(Collections.emptyList()).build())
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void localFiles() {
    Rel rel =
        LocalFiles.builder()
            .initialSchema(
                NamedStruct.of(
                    Collections.emptyList(), Type.Struct.builder().nullable(false).build()))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void namedScan() {
    Rel rel =
        NamedScan.builder()
            .from(
                b.namedScan(
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void extensionTable() {
    Rel rel = ExtensionTable.from(detail).build();
    verifyRoundTrip(rel);
  }

  @Test
  void filter() {
    Rel rel =
        Filter.builder()
            .from(b.filter(__ -> b.bool(true), commonTable))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void fetch() {
    Rel rel =
        Fetch.builder()
            .from(b.fetch(1, 2, commonTable))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void aggregate() {
    Rel rel =
        Aggregate.builder()
            .from(b.aggregate(b::grouping, __ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void sort() {
    Rel rel =
        Sort.builder()
            .from(b.sort(__ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void join() {
    Rel rel =
        Join.builder()
            .from(b.innerJoin(__ -> b.bool(true), commonTable, commonTable))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void project() {
    Rel rel =
        Project.builder()
            .from(b.project(__ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void set() {
    Rel rel =
        Set.builder()
            .from(b.set(Set.SetOp.UNION_ALL, commonTable))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void extensionSingleRel() {
    Rel rel = ExtensionSingle.from(detail, commonTable).commonExtension(commonExtension).build();
    verifyRoundTrip(rel);
  }

  @Test
  void extensionMultiRel() {
    Rel rel =
        ExtensionMulti.from(detail, commonTable, commonTable)
            .commonExtension(commonExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void extensionLeafRel() {
    Rel rel = ExtensionLeaf.from(detail).commonExtension(commonExtension).build();
    verifyRoundTrip(rel);
  }

  @Test
  void cross() {
    Rel rel =
        Cross.builder()
            .from(b.cross(commonTable, commonTable))
            .commonExtension(commonExtension)
            .relExtension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }
}
