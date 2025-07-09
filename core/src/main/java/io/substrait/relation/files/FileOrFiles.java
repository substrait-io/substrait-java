package io.substrait.relation.files;

import io.substrait.proto.ReadRel;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface FileOrFiles {

  Optional<PathType> pathType();

  enum PathType {
    URI_PATH,
    URI_PATH_GLOB,
    URI_FILE,
    URI_FOLDER
  }

  Optional<String> getPath();

  long getPartitionIndex();

  long getStart();

  long getLength();

  Optional<FileFormat> getFileFormat();

  static ImmutableFileOrFiles.Builder builder() {
    return ImmutableFileOrFiles.builder();
  }

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
