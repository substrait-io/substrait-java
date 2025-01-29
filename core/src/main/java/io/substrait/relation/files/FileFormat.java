package io.substrait.relation.files;

import java.util.Optional;
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
  abstract static class DelimiterSeparatedTextReadOptions implements FileFormat {
    public abstract String getFieldDelimiter();

    public abstract long getMaxLineSize();

    public abstract String getQuote();

    public abstract long getHeaderLinesToSkip();

    public abstract String getEscape();

    public abstract Optional<String> getValueTreatedAsNull();
  }

  @Value.Immutable
  abstract static class Extension implements FileFormat {
    public abstract com.google.protobuf.Any getExtension();
  }
}
