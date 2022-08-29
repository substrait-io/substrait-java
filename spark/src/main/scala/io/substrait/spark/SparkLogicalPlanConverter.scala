package io.substrait.spark

import io.substrait.relation.NamedScan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.JavaConverters._

object SparkLogicalPlanConverter {

  def convert(plan: LogicalPlan): io.substrait.relation.Rel = plan match {
    case project: Project =>
      val childRel = SparkLogicalPlanConverter.convert(project.child);
      val exprList = project.projectList.map(expr => SparkExpressionConverter.convert(expr, project.child, childRel.getRecordType)).asJava
      val projectRel = io.substrait.relation.Project.builder
        .expressions(exprList)
        .input(childRel)
        .build
      projectRel
    case logicalRelation: LogicalRelation =>
      val namedStruct = SparkTypeConverter.toNamedStruct(logicalRelation.schema)
      val tableNames = logicalRelation.catalogTable.get.identifier.unquotedString.split("\\.").toList.asJava
      val namedScan = NamedScan.builder.initialSchema(namedStruct).addAllNames(tableNames).build
      namedScan
    case _ =>
      null
  }
}