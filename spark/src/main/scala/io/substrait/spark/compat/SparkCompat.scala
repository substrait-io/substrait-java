package io.substrait.spark.compat

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Compatibility layer for Spark version-specific APIs. Implementations are provided in
 * variant-specific source directories.
 */
trait SparkCompat {

  /** Create a LogicalRelation with version-appropriate constructor */
  def createLogicalRelation(
      relation: HadoopFsRelation,
      output: Seq[AttributeReference],
      catalogTable: Option[org.apache.spark.sql.catalyst.catalog.CatalogTable],
      isStreaming: Boolean
  ): LogicalRelation

  /** Create a ListQuery with version-appropriate constructor */
  def createListQuery(
      plan: LogicalPlan,
      output: Seq[Attribute]): org.apache.spark.sql.catalyst.expressions.ListQuery

  /** Get SparkSession instance (returns AnyRef to work across versions) */
  def getOrCreateSparkSession(): AnyRef

  /** Create QueryExecution with version-appropriate SparkSession type */
  def createQueryExecution(
      spark: AnyRef,
      plan: LogicalPlan): org.apache.spark.sql.execution.QueryExecution

  /** Get config value from SparkSession */
  def getConf(spark: AnyRef, key: String): String

  /** Create InMemoryFileIndex with version-appropriate SparkSession type */
  def createInMemoryFileIndex(
      spark: AnyRef,
      paths: Seq[org.apache.hadoop.fs.Path],
      parameters: Map[String, String],
      userSpecifiedSchema: Option[org.apache.spark.sql.types.StructType]
  ): org.apache.spark.sql.execution.datasources.InMemoryFileIndex

  def createHadoopFsRelation(
      spark: AnyRef,
      location: org.apache.spark.sql.execution.datasources.InMemoryFileIndex,
      partitionSchema: org.apache.spark.sql.types.StructType,
      dataSchema: org.apache.spark.sql.types.StructType,
      bucketSpec: Option[org.apache.spark.sql.catalyst.catalog.BucketSpec],
      fileFormat: org.apache.spark.sql.execution.datasources.FileFormat,
      options: Map[String, String]
  ): org.apache.spark.sql.execution.datasources.HadoopFsRelation
}

object SparkCompat {

  /**
   * Get the version-specific implementation. This will be resolved at compile time based on the
   * variant being built.
   */
  lazy val instance: SparkCompat = new SparkCompatImpl()
}
