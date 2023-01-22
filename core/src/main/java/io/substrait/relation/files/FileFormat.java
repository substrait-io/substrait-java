package io.substrait.relation.files;

import org.immutables.value.Value;

@Value.Enclosing
public interface FileFormat {

  @Value.Immutable
  abstract static class ParquetReadOptions implements FileFormat {}

  @Value.Immutable
  abstract static class ArrowReadOptions implements FileFormat {}

  @Value.Immutable
  abstract static class OrcReadOptions implements FileFormat {}

  @Value.Immutable
  abstract static class DwrfReadOptions implements FileFormat {}

  @Value.Immutable
  abstract static class Extension implements FileFormat {
    public abstract com.google.protobuf.Any getExtension();
  }
}
