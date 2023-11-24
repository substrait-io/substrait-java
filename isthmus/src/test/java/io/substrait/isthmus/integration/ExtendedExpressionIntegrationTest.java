package io.substrait.isthmus.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ibm.icu.impl.ClassLoaderUtil;
import io.substrait.isthmus.ExtendedExpressionTestBase;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.proto.ExtendedExpression;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Optional;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class ExtendedExpressionIntegrationTest {

  @Test
  public void filterDataset() throws SqlParseException, IOException {
    URL resource = ClassLoaderUtil.getClassLoader().getResource("./tpch/data/nation.parquet");
    // Make sure you pass appropriate data, for example, if you pass N_NATIONKEY > 20 the engine
    // creates an i64 but casts it to i32 = 20, causing casting problems.
    String sqlExpression = "N_NATIONKEY > 9223372036854771827 - 9223372036854771807";
    ScanOptions options =
        new ScanOptions.Builder(/*batchSize*/ 32768)
            .columns(Optional.empty())
            .substraitFilter(getFilterExtendedExpression(sqlExpression))
            .build();
    try (BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory =
            new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                resource.toURI().toString());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()) {
      int count = 0;
      while (reader.loadNextBatch()) {
        count += reader.getVectorSchemaRoot().getRowCount();
      }
      assertEquals(4, count);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void projectDataset() throws SqlParseException, IOException {
    URL resource = ClassLoaderUtil.getClassLoader().getResource("./tpch/data/nation.parquet");
    // Make sure you pass appropriate data, for example, if you pass N_NATIONKEY + 20 the engine
    // creates an i64 but casts it to i32 = 20, causing casting problems.
    String sqlExpression = "N_NATIONKEY + 9888486986";
    ScanOptions options =
        new ScanOptions.Builder(/*batchSize*/ 32768)
            .columns(Optional.empty())
            .substraitProjection(getProjectExtendedExpression(sqlExpression))
            .build();
    try (BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory =
            new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                resource.toURI().toString());
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()) {
      int count = 0;
      Long sum = 0L;
      while (reader.loadNextBatch()) {
        count += reader.getVectorSchemaRoot().getRowCount();
        BigIntVector bigIntVector = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        for (int i = 0; i < bigIntVector.getValueCount(); i++) {
          sum += bigIntVector.get(i);
        }
      }
      assertEquals(25, count);
      assertEquals(24 * 25 / 2 + 9888486986L * count, sum);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static ByteBuffer getFilterExtendedExpression(String sqlExpression)
      throws IOException, SqlParseException {
    ExtendedExpression extendedExpression =
        new SqlToSubstrait()
            .executeSQLExpression(
                sqlExpression, ExtendedExpressionTestBase.tpchSchemaCreateStatements());
    byte[] extendedExpressions =
        Base64.getDecoder()
            .decode(Base64.getEncoder().encodeToString(extendedExpression.toByteArray()));
    ByteBuffer substraitExpressionFilter = ByteBuffer.allocateDirect(extendedExpressions.length);
    substraitExpressionFilter.put(extendedExpressions);
    return substraitExpressionFilter;
  }

  private static ByteBuffer getProjectExtendedExpression(String sqlExpression)
      throws IOException, SqlParseException {
    ExtendedExpression extendedExpression =
        new SqlToSubstrait()
            .executeSQLExpression(
                sqlExpression, ExtendedExpressionTestBase.tpchSchemaCreateStatements());
    byte[] extendedExpressions =
        Base64.getDecoder()
            .decode(Base64.getEncoder().encodeToString(extendedExpression.toByteArray()));
    ByteBuffer substraitExpressionProject = ByteBuffer.allocateDirect(extendedExpressions.length);
    substraitExpressionProject.put(extendedExpressions);
    return substraitExpressionProject;
  }
}
