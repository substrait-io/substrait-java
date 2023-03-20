package io.substrait.relation.extensions;

import com.google.protobuf.Any;
import io.substrait.relation.Extension;

/**
 * Default type to which AdvancedExtension.optimization are converted to.
 *
 * <p>Provide your own implementation of {@link
 * io.substrait.relation.ProtoRelConverter#enhancementFromAdvancedExtension(Any)} if you have
 * optimizations you wish to process.
 */
public class EmptyOptimization implements Extension.Optimization {
  @Override
  public Any toProto() {
    return com.google.protobuf.Any.pack(com.google.protobuf.Empty.getDefaultInstance());
  }
}
