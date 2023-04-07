package io.substrait.relation.extensions;

import com.google.protobuf.Any;
import io.substrait.relation.Extension;

/**
 * Default type to which {@link io.substrait.proto.AdvancedExtension#getOptimization()} data is
 * converted to by the {@link io.substrait.relation.ProtoRelConverter}
 */
public class EmptyOptimization implements Extension.Optimization {
  @Override
  public Any toProto() {
    return com.google.protobuf.Any.pack(com.google.protobuf.Empty.getDefaultInstance());
  }
}
