package io.substrait.relation.files;

import io.substrait.proto.ReadRel;
import java.util.Optional;
import org.immutables.value.Value;

/** Describes a single file or set of files to be read, including its path, layout and format. */
@Value.Immutable
public interface FileOrFiles {

  /**
   * Returns how the path should be interpreted, if a path is set.
   *
   * @return the optional path type
   */
  Optional<PathType> pathType();

  /** Describes how a {@link FileOrFiles} path should be interpreted. */
  enum PathType {
    /** A literal URI path. */
    URI_PATH,
    /** A URI path containing glob wildcards. */
    URI_PATH_GLOB,
    /** A URI identifying a single file. */
    URI_FILE,
    /** A URI identifying a folder whose contents are read. */
    URI_FOLDER
  }

  /**
   * Returns the path of the file(s), if set.
   *
   * @return the optional path
   */
  Optional<String> getPath();

  /**
   * Returns the partition index of this file within the read.
   *
   * @return the partition index
   */
  long getPartitionIndex();

  /**
   * Returns the byte offset at which to start reading.
   *
   * @return the start offset
   */
  long getStart();

  /**
   * Returns the number of bytes to read starting at {@link #getStart()}.
   *
   * @return the length in bytes
   */
  long getLength();

  /**
   * Returns the format of the file(s), if known.
   *
   * @return the optional file format
   */
  Optional<FileFormat> getFileFormat();

  /**
   * Creates a builder for {@link FileOrFiles}.
   *
   * @return a new builder
   */
  static ImmutableFileOrFiles.Builder builder() {
    return ImmutableFileOrFiles.builder();
  }

  /**
   * Converts this file specification to its protobuf representation.
   *
   * @return the protobuf {@link ReadRel.LocalFiles.FileOrFiles}
   */
  default ReadRel.LocalFiles.FileOrFiles toProto() {
    ReadRel.LocalFiles.FileOrFiles.Builder builder = ReadRel.LocalFiles.FileOrFiles.newBuilder();

    getFileFormat()
        .ifPresent(
            fileFormat -> {
              if (fileFormat instanceof FileFormat.ParquetReadOptions) {
                builder.setParquet(
                    ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.newBuilder().build());
              } else if (fileFormat instanceof FileFormat.ArrowReadOptions) {
                builder.setArrow(
                    ReadRel.LocalFiles.FileOrFiles.ArrowReadOptions.newBuilder().build());
              } else if (fileFormat instanceof FileFormat.OrcReadOptions) {
                builder.setOrc(ReadRel.LocalFiles.FileOrFiles.OrcReadOptions.newBuilder().build());
              } else if (fileFormat instanceof FileFormat.DwrfReadOptions) {
                builder.setDwrf(
                    ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions.newBuilder().build());
              } else if (fileFormat instanceof FileFormat.DelimiterSeparatedTextReadOptions) {
                FileFormat.DelimiterSeparatedTextReadOptions options =
                    (FileFormat.DelimiterSeparatedTextReadOptions) fileFormat;
                ReadRel.LocalFiles.FileOrFiles.DelimiterSeparatedTextReadOptions.Builder
                    optionsBuilder =
                        ReadRel.LocalFiles.FileOrFiles.DelimiterSeparatedTextReadOptions
                            .newBuilder()
                            .setFieldDelimiter(options.getFieldDelimiter())
                            .setMaxLineSize(options.getMaxLineSize())
                            .setQuote(options.getQuote())
                            .setHeaderLinesToSkip(options.getHeaderLinesToSkip())
                            .setEscape(options.getEscape());
                options.getValueTreatedAsNull().ifPresent(optionsBuilder::setValueTreatedAsNull);
                builder.setText(optionsBuilder.build());
              } else if (fileFormat instanceof FileFormat.Extension) {
                FileFormat.Extension options = (FileFormat.Extension) fileFormat;
                builder.setExtension(options.getExtension());
              } else {
                throw new UnsupportedOperationException(
                    "Unable to convert file format of " + fileFormat.getClass());
              }
            });

    pathType()
        .ifPresent(
            pathType ->
                getPath()
                    .ifPresent(
                        path -> {
                          if (pathType == PathType.URI_PATH) {
                            builder.setUriPath(path);
                          } else if (pathType == PathType.URI_PATH_GLOB) {
                            builder.setUriPathGlob(path);
                          } else if (pathType == PathType.URI_FILE) {
                            builder.setUriFile(path);
                          } else if (pathType == PathType.URI_FOLDER) {
                            builder.setUriFolder(path);
                          }
                        }));

    return builder
        .setPartitionIndex(getPartitionIndex())
        .setStart(getStart())
        .setLength(getLength())
        .build();
  }
}
