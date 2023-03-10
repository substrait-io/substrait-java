package io.substrait.relation.extensions;

import com.google.protobuf.Any;
import io.substrait.relation.Extension;

/**
 * Default type to which AdvancedExtension.enhancements are converted to.
 *
 * <p>Provide your own implementation of {@link
 * io.substrait.relation.ProtoRelConverter#enhancementFromAdvancedExtension(Any)} if you have
 * enhancements you wish to process.
 */
public class EmptyEnhancement implements Extension.Enhancement {
  @Override
  public Any toProto() {
    return com.google.protobuf.Any.pack(com.google.protobuf.Empty.getDefaultInstance());
  }
}
