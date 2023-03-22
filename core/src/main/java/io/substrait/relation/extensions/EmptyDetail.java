package io.substrait.relation.extensions;

import com.google.protobuf.Any;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Collections;
import java.util.List;

public class EmptyDetail
    implements Extension.LeafRelDetail,
        Extension.MultiRelDetail,
        Extension.SingleRelDetail,
        Extension.ExtensionTableDetail {

  @Override
  public Any toProto() {
    return com.google.protobuf.Any.pack(com.google.protobuf.Empty.getDefaultInstance());
  }

  @Override
  public Type.Struct deriveRecordType() {
    return TypeCreator.NULLABLE.struct();
  }

  @Override
  public Type.Struct deriveRecordType(Rel input) {
    return TypeCreator.NULLABLE.struct();
  }

  @Override
  public Type.Struct deriveRecordType(List<Rel> inputs) {
    return TypeCreator.NULLABLE.struct();
  }

  @Override
  public NamedStruct deriveSchema() {
    return NamedStruct.of(Collections.emptyList(), Type.Struct.builder().nullable(true).build());
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof EmptyDetail;
  }

  @Override
  public int hashCode() {
    return EmptyDetail.class.hashCode();
  }

  @Override
  public String toString() {
    return "EmptyDetail{}";
  }
}
