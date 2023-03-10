package io.substrait.relation.utils;

import com.google.protobuf.Any;
import io.substrait.relation.Extension;
import java.util.Objects;

/**
 * For use in {@link io.substrait.relation.ProtoRelConverterTest}
 *
 * <p>Used to verify serde of {@link com.google.protobuf.Any} fields in the spec
 */
public class StringHolder implements Extension.Enhancement, Extension.Optimization {

  private final String value;

  public StringHolder(String value) {
    this.value = value;
  }

  @Override
  public Any toProto() {
    return com.google.protobuf.Any.pack(com.google.protobuf.StringValue.of(this.value));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StringHolder that = (StringHolder) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
