package io.substrait.spark.compat

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import io.substrait.relation

class SparkCompatImpl extends SparkCompat {

  override def createScalarSubquery(plan: LogicalPlan): ScalarSubquery = {
    // Spark 3.4 requires exprId parameter
    ScalarSubquery(plan, exprId = NamedExpression.newExprId)
  }

  override def createAggregate(
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression],
      child: LogicalPlan
  ): Aggregate = {
    // Spark 3.4 uses 3-parameter constructor
    Aggregate(groupingExpressions, aggregateExpressions, child)
  }

  override def createLogicalRelation(
      relation: HadoopFsRelation,
      output: Seq[AttributeReference],
      catalogTable: Option[org.apache.spark.sql.catalyst.catalog.CatalogTable],
      isStreaming: Boolean
  ): LogicalRelation = {
    // Spark 3.4 uses 4-parameter constructor (no stream parameter)
    new LogicalRelation(relation, output, catalogTable, isStreaming)
  }

  override def createListQuery(
      plan: LogicalPlan,
      output: Seq[Attribute]): org.apache.spark.sql.catalyst.expressions.ListQuery = {
    // Spark 3.4 doesn't have numCols parameter
    org.apache.spark.sql.catalyst.expressions.ListQuery(plan, childOutputs = output)
  }

  override def getOrCreateSparkSession(): AnyRef = {
    org.apache.spark.sql.SparkSession.builder().getOrCreate()
  }

  override def createQueryExecution(
      spark: AnyRef,
      plan: LogicalPlan): org.apache.spark.sql.execution.QueryExecution = {
    new org.apache.spark.sql.execution.QueryExecution(
      spark.asInstanceOf[org.apache.spark.sql.SparkSession],
      plan)
  }

  override def getConf(spark: AnyRef, key: String): String = {
    spark.asInstanceOf[org.apache.spark.sql.SparkSession].conf.get(key)
  }

  override def createInMemoryFileIndex(
      spark: AnyRef,
      paths: Seq[org.apache.hadoop.fs.Path],
      parameters: Map[String, String],
      userSpecifiedSchema: Option[org.apache.spark.sql.types.StructType]
  ): org.apache.spark.sql.execution.datasources.InMemoryFileIndex = {
    new org.apache.spark.sql.execution.datasources.InMemoryFileIndex(
      spark.asInstanceOf[org.apache.spark.sql.SparkSession],
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
    )(spark.asInstanceOf[org.apache.spark.sql.SparkSession])
  }
}

object WindowGroupLimitCase {
  def unapply(l: LogicalPlan): Option[LogicalPlan] = None
}
