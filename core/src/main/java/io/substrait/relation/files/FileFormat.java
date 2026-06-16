package io.substrait.relation.files;

import java.util.Optional;
import org.immutables.value.Value;

/** The format-specific read options for a file referenced by a {@link FileOrFiles}. */
@Value.Enclosing
public interface FileFormat {

  /** Read options for Parquet files. */
  @Value.Immutable
  abstract class ParquetReadOptions implements FileFormat {
    /**
     * Creates a builder for {@link ParquetReadOptions}.
     *
     * @return a new builder
     */
    public static ImmutableFileFormat.ParquetReadOptions.Builder builder() {
      return ImmutableFileFormat.ParquetReadOptions.builder();
    }
  }

  /** Read options for Arrow files. */
  @Value.Immutable
  abstract class ArrowReadOptions implements FileFormat {
    /**
     * Creates a builder for {@link ArrowReadOptions}.
     *
     * @return a new builder
     */
    public static ImmutableFileFormat.ArrowReadOptions.Builder builder() {
      return ImmutableFileFormat.ArrowReadOptions.builder();
    }
  }

  /** Read options for ORC files. */
  @Value.Immutable
  abstract class OrcReadOptions implements FileFormat {
    /**
     * Creates a builder for {@link OrcReadOptions}.
     *
     * @return a new builder
     */
    public static ImmutableFileFormat.OrcReadOptions.Builder builder() {
      return ImmutableFileFormat.OrcReadOptions.builder();
    }
  }

  /** Read options for DWRF files. */
  @Value.Immutable
  abstract class DwrfReadOptions implements FileFormat {
    /**
     * Creates a builder for {@link DwrfReadOptions}.
     *
     * @return a new builder
     */
    public static ImmutableFileFormat.DwrfReadOptions.Builder builder() {
      return ImmutableFileFormat.DwrfReadOptions.builder();
    }
  }

  /** Read options for delimiter-separated text files such as CSV. */
  @Value.Immutable
  abstract class DelimiterSeparatedTextReadOptions implements FileFormat {
    /**
     * Returns the field delimiter separating columns.
     *
     * @return the field delimiter
     */
    public abstract String getFieldDelimiter();

    /**
     * Returns the maximum supported line size in bytes.
     *
     * @return the maximum line size
     */
    public abstract long getMaxLineSize();

    /**
     * Returns the quote character used to wrap field values.
     *
     * @return the quote character
     */
    public abstract String getQuote();

    /**
     * Returns the number of header lines to skip before reading data.
     *
     * @return the number of header lines to skip
     */
    public abstract long getHeaderLinesToSkip();

    /**
     * Returns the escape character used within field values.
     *
     * @return the escape character
     */
    public abstract String getEscape();

    /**
     * Returns the literal value treated as null, if any.
     *
     * @return the optional null sentinel value
     */
    public abstract Optional<String> getValueTreatedAsNull();

    /**
     * Creates a builder for {@link DelimiterSeparatedTextReadOptions}.
     *
     * @return a new builder
     */
    public static ImmutableFileFormat.DelimiterSeparatedTextReadOptions.Builder builder() {
      return ImmutableFileFormat.DelimiterSeparatedTextReadOptions.builder();
    }
  }

  /** Read options described by an extension-defined message. */
  @Value.Immutable
  abstract class Extension implements FileFormat {
    /**
     * Returns the extension-defined read options message.
     *
     * @return the extension message
     */
    public abstract com.google.protobuf.Any getExtension();

    /**
     * Creates a builder for {@link Extension}.
     *
     * @return a new builder
     */
    public static ImmutableFileFormat.Extension.Builder builder() {
      return ImmutableFileFormat.Extension.builder();
    }
  }
}
