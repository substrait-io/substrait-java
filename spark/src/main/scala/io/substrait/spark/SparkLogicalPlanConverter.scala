package io.substrait.spark

import io.substrait.relation.NamedScan
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

object SparkLogicalPlanConverter {

  def convert(plan: LogicalPlan): io.substrait.relation.Rel = plan match {
    case _: Project =>
      convertProject(plan)
    case _: LogicalRelation | _: DataSourceV2ScanRelation | _: HiveTableRelation =>
      convertReadOperator(plan)
    case _ =>
      throw new UnsupportedOperationException(String.format("Unable to convert the plan to a substrait rel: " + plan))

  }

  /**
   * Project Operator: https://substrait.io/relations/logical_relations/#project-operation
   *
   * @param plan
   * @return
   */
  def convertProject(plan: LogicalPlan): io.substrait.relation.Project = plan match {
    case project: Project =>
      val childRel = SparkLogicalPlanConverter.convert(project.child);
      val exprList = project.projectList.map(expr => SparkExpressionConverter.convert(expr, project.child, childRel.getRecordType)).asJava
      val projectRel = io.substrait.relation.Project.builder
        .expressions(exprList)
        .input(childRel)
        .build
      projectRel
  }

  def buildNamedScan(schema: StructType, tableNames: List[String]): NamedScan = {
    val namedStruct = SparkTypeConverter.toNamedStruct(schema)
    val namedScan = NamedScan.builder.initialSchema(namedStruct).addAllNames(tableNames.asJava).build
    namedScan
  }

  /**
   * Read Operator: https://substrait.io/relations/logical_relations/#read-operator
   *
   * @param plan
   * @return
   */
  def convertReadOperator(plan: LogicalPlan): io.substrait.relation.AbstractReadRel = {
    var schema: StructType = null
    var tableNames: List[String] = null;
    plan match {
      case logicalRelation: LogicalRelation =>
        schema = logicalRelation.schema
        tableNames = logicalRelation.catalogTable.get.identifier.unquotedString.split("\\.").toList
        buildNamedScan(schema, tableNames)
      case dataSourceV2ScanRelation: DataSourceV2ScanRelation =>
        schema = dataSourceV2ScanRelation.schema
        tableNames = dataSourceV2ScanRelation.relation.identifier.get.toString.split("\\.").toList
        buildNamedScan(schema, tableNames)
      case dataSourceV2Relation: DataSourceV2Relation =>
        schema = dataSourceV2Relation.schema
        tableNames = dataSourceV2Relation.identifier.get.toString.split("\\.").toList
        buildNamedScan(schema, tableNames)
      case hiveTableRelation: HiveTableRelation =>
        schema = hiveTableRelation.schema
        tableNames = hiveTableRelation.tableMeta.identifier.unquotedString.split("\\.").toList
        buildNamedScan(schema, tableNames)
      //TODO: LocalRelation,Range=>Virtual Table,LogicalRelation(HadoopFsRelation)=>LocalFiles

      case _ =>
        throw new UnsupportedOperationException(String.format("Unable to convert the plan to a substrait NamedScan: " + plan))
    }

  }
}