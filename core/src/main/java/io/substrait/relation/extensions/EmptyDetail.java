package io.substrait.relation.extensions;

import com.google.protobuf.Any;
import io.substrait.relation.Extension;

public class EmptyDetail
    implements Extension.LeafRelDetail, Extension.MultiRelDetail, Extension.SingleRelDetail {

  @Override
  public Any toProto() {
    return com.google.protobuf.Any.pack(com.google.protobuf.Empty.getDefaultInstance());
  }
}
