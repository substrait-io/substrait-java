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

public class LocalFilesRoundtripTest extends TestBase {

  private void assertLocalFilesRoundtrip(FileOrFiles file) {
    var builder =
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

    var localFiles = builder.build();
    var protoFileRel = relProtoConverter.toProto(localFiles);
    assertTrue(protoFileRel.getRead().hasFilter());
    assertEquals(protoFileRel, relProtoConverter.toProto(protoRelConverter.from(protoFileRel)));
  }

  private ImmutableFileOrFiles.Builder setPath(
      ImmutableFileOrFiles.Builder builder,
      ReadRel.LocalFiles.FileOrFiles.PathTypeCase pathTypeCase) {
    return switch (pathTypeCase) {
      case URI_PATH -> builder.pathType(FileOrFiles.PathType.URI_PATH).path("path");
      case URI_PATH_GLOB -> builder.pathType(FileOrFiles.PathType.URI_PATH_GLOB).path("path");
      case URI_FILE -> builder.pathType(FileOrFiles.PathType.URI_FILE).path("path");
      case URI_FOLDER -> builder.pathType(FileOrFiles.PathType.URI_FOLDER).path("path");
      case PATHTYPE_NOT_SET -> builder;
    };
  }

  private ImmutableFileOrFiles.Builder setFileFormat(
      ImmutableFileOrFiles.Builder builder,
      ReadRel.LocalFiles.FileOrFiles.FileFormatCase fileFormatCase) {
    return switch (fileFormatCase) {
      case PARQUET -> builder.fileFormat(FileFormat.ParquetReadOptions.builder().build());
      case ARROW -> builder.fileFormat(FileFormat.ArrowReadOptions.builder().build());
      case ORC -> builder.fileFormat(FileFormat.OrcReadOptions.builder().build());
      case DWRF -> builder.fileFormat(FileFormat.DwrfReadOptions.builder().build());
      case TEXT -> builder.fileFormat(
          FileFormat.DelimiterSeparatedTextReadOptions.builder()
              .fieldDelimiter("|")
              .maxLineSize(1000)
              .quote("\"")
              .headerLinesToSkip(1)
              .escape("\\")
              .build());
      case EXTENSION -> builder.fileFormat(
          FileFormat.Extension.builder()
              .extension(com.google.protobuf.Any.newBuilder().build())
              .build());
      case FILEFORMAT_NOT_SET -> builder;
    };
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
