package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.proto.ReadRel;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.files.FileFormat;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.relation.files.ImmutableFileOrFiles;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class LocalFilesRoundtripTest extends TestBase {

  private void assertLocalFilesRoundtrip(final FileOrFiles file) {
    final io.substrait.relation.ImmutableLocalFiles.Builder builder =
        LocalFiles.builder()
            .initialSchema(
                NamedStruct.builder()
                    .addNames("id")
                    .struct(
                        Type.Struct.builder()
                            .nullable(false)
                            .addFields(TypeCreator.REQUIRED.I32)
                            .build())
                    .build())
            .addItems(file);

    defaultExtensionCollection.scalarFunctions().stream()
        .filter(s -> s.name().equalsIgnoreCase("equal"))
        .findFirst()
        .map(
            declaration ->
                ExpressionCreator.scalarFunction(
                    declaration,
                    TypeCreator.REQUIRED.BOOLEAN,
                    FieldReference.builder()
                        .addSegments(FieldReference.StructField.of(0))
                        .type(TypeCreator.REQUIRED.I32)
                        .build(),
                    ExpressionCreator.i32(false, 1)))
        .ifPresent(builder::filter);

    final io.substrait.relation.ImmutableLocalFiles localFiles = builder.build();
    final io.substrait.proto.Rel protoFileRel = relProtoConverter.toProto(localFiles);
    assertTrue(protoFileRel.getRead().hasFilter());
    assertEquals(protoFileRel, relProtoConverter.toProto(protoRelConverter.from(protoFileRel)));
  }

  private ImmutableFileOrFiles.Builder setPath(
      final ImmutableFileOrFiles.Builder builder,
      final ReadRel.LocalFiles.FileOrFiles.PathTypeCase pathTypeCase) {
    switch (pathTypeCase) {
      case URI_PATH:
        return builder.pathType(FileOrFiles.PathType.URI_PATH).path("path");
      case URI_PATH_GLOB:
        return builder.pathType(FileOrFiles.PathType.URI_PATH_GLOB).path("path");
      case URI_FILE:
        return builder.pathType(FileOrFiles.PathType.URI_FILE).path("path");
      case URI_FOLDER:
        return builder.pathType(FileOrFiles.PathType.URI_FOLDER).path("path");
      case PATHTYPE_NOT_SET:
        return builder;
      default:
        throw new IllegalArgumentException("Unknown path type case: " + pathTypeCase);
    }
  }

  private ImmutableFileOrFiles.Builder setFileFormat(
      final ImmutableFileOrFiles.Builder builder,
      final ReadRel.LocalFiles.FileOrFiles.FileFormatCase fileFormatCase) {
    switch (fileFormatCase) {
      case PARQUET:
        return builder.fileFormat(FileFormat.ParquetReadOptions.builder().build());
      case ARROW:
        return builder.fileFormat(FileFormat.ArrowReadOptions.builder().build());
      case ORC:
        return builder.fileFormat(FileFormat.OrcReadOptions.builder().build());
      case DWRF:
        return builder.fileFormat(FileFormat.DwrfReadOptions.builder().build());
      case TEXT:
        return builder.fileFormat(
            FileFormat.DelimiterSeparatedTextReadOptions.builder()
                .fieldDelimiter("|")
                .maxLineSize(1000)
                .quote("\"")
                .headerLinesToSkip(1)
                .escape("\\")
                .build());
      case EXTENSION:
        return builder.fileFormat(
            FileFormat.Extension.builder()
                .extension(com.google.protobuf.Any.newBuilder().build())
                .build());
      case FILEFORMAT_NOT_SET:
        return builder;
      default:
        throw new IllegalArgumentException("Unknown file format case: " + fileFormatCase);
    }
  }

  @Test
  void localFilesRoundtrip() {
    Arrays.stream(ReadRel.LocalFiles.FileOrFiles.FileFormatCase.values())
        .forEach(
            fileFormatCase ->
                Arrays.stream(ReadRel.LocalFiles.FileOrFiles.PathTypeCase.values())
                    .forEach(
                        pathTypeCase ->
                            assertLocalFilesRoundtrip(
                                setFileFormat(
                                        setPath(FileOrFiles.builder(), pathTypeCase),
                                        fileFormatCase)
                                    .partitionIndex(0)
                                    .start(2)
                                    .length(10000)
                                    .build())));
  }
}
