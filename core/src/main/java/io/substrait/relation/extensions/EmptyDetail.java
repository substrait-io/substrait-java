package io.substrait.relation.extensions;

import com.google.protobuf.Any;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;

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
  public Type.Struct deriveRecordType(Rel... inputs) {
    return TypeCreator.NULLABLE.struct();
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
