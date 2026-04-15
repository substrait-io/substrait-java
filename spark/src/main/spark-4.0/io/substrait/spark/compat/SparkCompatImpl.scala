package io.substrait.spark.compat

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, WindowGroupLimit}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

class SparkCompatImpl extends SparkCompat {

  override def createLogicalRelation(
      relation: HadoopFsRelation,
      output: Seq[AttributeReference],
      catalogTable: Option[org.apache.spark.sql.catalyst.catalog.CatalogTable],
      isStreaming: Boolean
  ): LogicalRelation = {
    // Spark 4.0 requires stream parameter (None for non-streaming)
    new LogicalRelation(relation, output, catalogTable, isStreaming, None)
  }

  override def createListQuery(
      plan: LogicalPlan,
      output: Seq[Attribute]): org.apache.spark.sql.catalyst.expressions.ListQuery = {
    // Spark 4.0 has numCols parameter
    org.apache.spark.sql.catalyst.expressions.ListQuery(plan, numCols = output.length)
  }

  override def getOrCreateSparkSession(): AnyRef = {
    org.apache.spark.sql.classic.SparkSession.builder().getOrCreate()
  }

  override def createQueryExecution(
      spark: AnyRef,
      plan: LogicalPlan): org.apache.spark.sql.execution.QueryExecution = {
    new org.apache.spark.sql.execution.QueryExecution(
      spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession],
      plan)
  }

  override def getConf(spark: AnyRef, key: String): String = {
    spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession].conf.get(key)
  }

  override def createInMemoryFileIndex(
      spark: AnyRef,
      paths: Seq[org.apache.hadoop.fs.Path],
      parameters: Map[String, String],
      userSpecifiedSchema: Option[org.apache.spark.sql.types.StructType]
  ): org.apache.spark.sql.execution.datasources.InMemoryFileIndex = {
    new org.apache.spark.sql.execution.datasources.InMemoryFileIndex(
      spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession],
      paths,
      parameters,
      userSpecifiedSchema
    )
  }

  override def createHadoopFsRelation(
      spark: AnyRef,
      location: org.apache.spark.sql.execution.datasources.InMemoryFileIndex,
      partitionSchema: org.apache.spark.sql.types.StructType,
      dataSchema: org.apache.spark.sql.types.StructType,
      bucketSpec: Option[org.apache.spark.sql.catalyst.catalog.BucketSpec],
      fileFormat: org.apache.spark.sql.execution.datasources.FileFormat,
      options: Map[String, String]
  ): org.apache.spark.sql.execution.datasources.HadoopFsRelation = {
    org.apache.spark.sql.execution.datasources.HadoopFsRelation(
      location,
      partitionSchema,
      dataSchema,
      bucketSpec,
      fileFormat,
      options
    )(spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession])
  }
}

object WindowGroupLimitCase {
  def unapply(l: LogicalPlan): Option[LogicalPlan] = l match {
    case w: WindowGroupLimit => Some(w.child)
    case _ => None
  }
}
