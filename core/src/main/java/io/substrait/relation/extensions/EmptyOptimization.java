package io.substrait.relation.extensions;

import com.google.protobuf.Any;
import io.substrait.proto.AdvancedExtension;
import io.substrait.relation.Extension;
import io.substrait.relation.RelProtoConverter;

/**
 * Default type to which {@link AdvancedExtension#getOptimizationList()} data is converted to by the
 * {@link io.substrait.relation.ProtoRelConverter}
 */
public class EmptyOptimization implements Extension.Optimization {
  @Override
  public Any toProto(RelProtoConverter converter) {
    return com.google.protobuf.Any.pack(com.google.protobuf.Empty.getDefaultInstance());
  }
}
