package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.test.SharedSparkSession

import io.substrait.extension.ExtensionCollector
import io.substrait.relation.{ExtensionWrite, RelProtoConverter}
import io.substrait.relation.AbstractWriteRel.{CreateMode, WriteOp}
import org.apache.hadoop.fs.Path

class FileWriteSuite extends SparkFunSuite with SharedSparkSession {

  private def withTarget(f: InsertIntoHadoopFsRelationCommand => Unit): Unit = {
    withTable("file_write_target") {
      spark.sql("CREATE TABLE file_write_target (id INT) USING PARQUET")
      spark.sql("INSERT INTO file_write_target VALUES (1), (2)")
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("file_write_target"))
      val child = spark.sql("SELECT 3 AS id").queryExecution.optimizedPlan
      f(
        InsertIntoHadoopFsRelationCommand(
          outputPath = new Path(table.location),
          staticPartitions = Map.empty,
          ifPartitionNotExists = false,
          partitionColumns = Seq.empty,
          bucketSpec = None,
          fileFormat = new ParquetFileFormat(),
          options = Map.empty,
          query = child,
          mode = SaveMode.Append,
          catalogTable = Some(table),
          fileIndex = None,
          outputColumnNames = Seq("id")
        ))
    }
  }

  private def targetRows: Seq[Row] =
    spark.sql("SELECT id FROM file_write_target ORDER BY id").collect().toSeq

  private def convertWrite(command: InsertIntoHadoopFsRelationCommand): ExtensionWrite =
    new ToSubstraitRel().visit(command).asInstanceOf[ExtensionWrite]

  private def importProto(write: ExtensionWrite): LogicalPlan = {
    val collector = new ExtensionCollector
    val bytes = new RelProtoConverter(collector).toProto(write).toByteArray
    val decoded = new FileHolderHandlingProtoRelConverter(collector)
      .from(io.substrait.proto.Rel.parseFrom(bytes))
    new ToLogicalPlan(spark).convert(decoded)
  }

  test("append writes preserve existing rows through the file extension protobuf") {
    withTarget {
      command =>
        val write = convertWrite(command)
        assertResult(CreateMode.UNSPECIFIED)(write.getCreateMode)
        val plan = importProto(write)
        spark.sessionState.executePlan(plan).executedPlan.execute()
        assertResult(Seq(Row(1), Row(2), Row(3)))(targetRows)
    }
  }

  test("legacy append file extensions remain executable") {
    withTarget {
      command =>
        val write = ExtensionWrite
          .builder()
          .from(convertWrite(command))
          .createMode(CreateMode.APPEND_IF_EXISTS)
          .build()
        spark.sessionState.executePlan(importProto(write)).executedPlan.execute()
        assertResult(Seq(Row(1), Row(2), Row(3)))(targetRows)
    }
  }

  test("reject filesystem save modes that cannot be represented as INSERT") {
    withTarget {
      command =>
        Seq(SaveMode.Overwrite, SaveMode.Ignore, SaveMode.ErrorIfExists).foreach {
          mode =>
            val error = intercept[UnsupportedOperationException] {
              convertWrite(command.copy(mode = mode))
            }
            assert(error.getMessage.contains(s"SaveMode.Append, found $mode"))
            assertResult(Seq(Row(1), Row(2)))(targetRows)
        }
    }
  }

  test("reject partition and bucket metadata that the file extension cannot carry") {
    withTarget {
      command =>
        val partitioned = spark.sql("SELECT 3 AS id, 10 AS part").queryExecution.optimizedPlan
        val commands = Seq(
          command.copy(staticPartitions = Map("part" -> "10")),
          command.copy(ifPartitionNotExists = true),
          command.copy(
            partitionColumns = Seq(partitioned.output.last),
            query = partitioned,
            outputColumnNames = Seq("id", "part")),
          command.copy(bucketSpec = Some(BucketSpec(2, Seq("id"), Seq.empty)))
        )
        commands.foreach {
          unsupported =>
            val error = intercept[UnsupportedOperationException] {
              convertWrite(unsupported)
            }
            assert(error.getMessage.contains("filesystem writes are not supported"))
            assertResult(Seq(Row(1), Row(2)))(targetRows)
        }
    }
  }

  test("reject legacy file save modes before constructing an executable INSERT") {
    withTarget {
      command =>
        Seq(CreateMode.REPLACE_IF_EXISTS, CreateMode.IGNORE_IF_EXISTS, CreateMode.ERROR_IF_EXISTS)
          .foreach {
            mode =>
              val write =
                ExtensionWrite.builder().from(convertWrite(command)).createMode(mode).build()
              val error = intercept[UnsupportedOperationException] {
                importProto(write)
              }
              assert(error.getMessage.contains(s"INSERT does not support create mode $mode"))
              assertResult(Seq(Row(1), Row(2)))(targetRows)
          }
    }
  }

  test("reject file UPDATE instead of replacing the entire target") {
    withTarget {
      command =>
        val write =
          ExtensionWrite.builder().from(convertWrite(command)).operation(WriteOp.UPDATE).build()
        val error = intercept[UnsupportedOperationException] {
          importProto(write)
        }
        assert(error.getMessage.contains("Write mode UPDATE not supported"))
        assertResult(Seq(Row(1), Row(2)))(targetRows)
    }
  }
}
