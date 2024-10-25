/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.substrait.spark.logical

import io.substrait.spark.{DefaultRelVisitor, SparkExtension, ToSubstraitType}
import io.substrait.spark.expression._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}

import io.substrait.`type`.{StringTypeVisitor, Type}
import io.substrait.{expression => exp}
import io.substrait.expression.{Expression => SExpression}
import io.substrait.plan.Plan
import io.substrait.relation
import io.substrait.relation.Expand.{ConsistentField, SwitchingField}
import io.substrait.relation.LocalFiles
import io.substrait.relation.Set.SetOp
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer

/**
 * RelVisitor to convert Substrait Rel plan to [[LogicalPlan]]. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
class ToLogicalPlan(spark: SparkSession) extends DefaultRelVisitor[LogicalPlan] {

  private val expressionConverter =
    new ToSparkExpression(ToScalarFunction(SparkExtension.SparkScalarFunctions), Some(this))

  private def fromMeasure(measure: relation.Aggregate.Measure): AggregateExpression = {
    // this functions is called in createParentwithChild
    val function = measure.getFunction
    var arguments = function.arguments().asScala.zipWithIndex.map {
      case (arg, i) =>
        arg.accept(function.declaration(), i, expressionConverter)
    }
    if (function.declaration.name == "count" && function.arguments.size == 0) {
      // HACK - count() needs to be rewritten as count(1)
      arguments = ArrayBuffer(Literal(1))
    }

    val aggregateFunction = SparkExtension.toAggregateFunction
      .getSparkExpressionFromSubstraitFunc(function.declaration.key, function.outputType)
      .map(sig => sig.makeCall(arguments))
      .map(_.asInstanceOf[AggregateFunction])
      .getOrElse({
        val msg = String.format(
          "Unable to convert Aggregate function %s(%s).",
          function.declaration.name,
          function.arguments.asScala
            .map {
              case ea: exp.EnumArg => ea.value.toString
              case e: SExpression => e.getType.accept(new StringTypeVisitor)
              case t: Type => t.accept(new StringTypeVisitor)
              case a => throw new IllegalStateException("Unexpected value: " + a)
            }
            .mkString(", ")
        )
        throw new IllegalArgumentException(msg)
      })

    val filter = Option(measure.getPreMeasureFilter.orElse(null))
      .map(_.accept(expressionConverter))

    AggregateExpression(
      aggregateFunction,
      ToAggregateFunction.toSpark(function.aggregationPhase()),
      ToAggregateFunction.toSpark(function.invocation()),
      filter
    )
  }

  private def toNamedExpression(e: Expression): NamedExpression = e match {
    case ne: NamedExpression => ne
    case other => Alias(other, toPrettySQL(other))()
  }

  override def visit(aggregate: relation.Aggregate): LogicalPlan = {
    require(aggregate.getGroupings.size() == 1)
    val child = aggregate.getInput.accept(this)
    withChild(child) {
      val groupBy = aggregate.getGroupings
        .get(0)
        .getExpressions
        .asScala
        .map(expr => expr.accept(expressionConverter))

      val outputs = groupBy.map(toNamedExpression)
      val aggregateExpressions =
        aggregate.getMeasures.asScala.map(fromMeasure).map(toNamedExpression)
      Aggregate(groupBy, outputs ++= aggregateExpressions, child)
    }
  }

  override def visit(join: relation.Join): LogicalPlan = {
    val left = join.getLeft.accept(this)
    val right = join.getRight.accept(this)
    withChild(left, right) {
      val condition = Option(join.getCondition.orElse(null))
        .map(_.accept(expressionConverter))

      val joinType = join.getJoinType match {
        case relation.Join.JoinType.INNER => Inner
        case relation.Join.JoinType.LEFT => LeftOuter
        case relation.Join.JoinType.RIGHT => RightOuter
        case relation.Join.JoinType.OUTER => FullOuter
        case relation.Join.JoinType.SEMI => LeftSemi
        case relation.Join.JoinType.ANTI => LeftAnti
        case relation.Join.JoinType.UNKNOWN =>
          throw new UnsupportedOperationException("Unknown join type is not supported")
      }
      Join(left, right, joinType, condition, hint = JoinHint.NONE)
    }
  }

  override def visit(join: relation.Cross): LogicalPlan = {
    val left = join.getLeft.accept(this)
    val right = join.getRight.accept(this)
    withChild(left, right) {
      // TODO: Support different join types here when join types are added to cross rel for BNLJ
      // Currently, this will change both cross and inner join types to inner join
      Join(left, right, Inner, Option(null), hint = JoinHint.NONE)
    }
  }

  private def toSortOrder(sortField: SExpression.SortField): SortOrder = {
    val expression = sortField.expr().accept(expressionConverter)
    val (direction, nullOrdering) = sortField.direction() match {
      case SExpression.SortDirection.ASC_NULLS_FIRST => (Ascending, NullsFirst)
      case SExpression.SortDirection.DESC_NULLS_FIRST => (Descending, NullsFirst)
      case SExpression.SortDirection.ASC_NULLS_LAST => (Ascending, NullsLast)
      case SExpression.SortDirection.DESC_NULLS_LAST => (Descending, NullsLast)
      case other =>
        throw new RuntimeException(s"Unexpected Expression.SortDirection enum: $other !")
    }
    SortOrder(expression, direction, nullOrdering, Seq.empty)
  }
  override def visit(fetch: relation.Fetch): LogicalPlan = {
    val child = fetch.getInput.accept(this)
    val limit = fetch.getCount.getAsLong.intValue()
    val offset = fetch.getOffset.intValue()
    val toLiteral = (i: Int) => Literal(i, IntegerType)
    if (limit >= 0) {
      val limitExpr = toLiteral(limit)
      if (offset > 0) {
        GlobalLimit(
          limitExpr,
          Offset(toLiteral(offset), LocalLimit(toLiteral(offset + limit), child)))
      } else {
        GlobalLimit(limitExpr, LocalLimit(limitExpr, child))
      }
    } else {
      Offset(toLiteral(offset), child)
    }
  }
  override def visit(sort: relation.Sort): LogicalPlan = {
    val child = sort.getInput.accept(this)
    withChild(child) {
      val sortOrders = sort.getSortFields.asScala.map(toSortOrder)
      Sort(sortOrders, global = true, child)
    }
  }

  override def visit(project: relation.Project): LogicalPlan = {
    val child = project.getInput.accept(this)
    val (output, createProject) = child match {
      case a: Aggregate => (a.aggregateExpressions, false)
      case other => (other.output, true)
    }

    withOutput(output) {
      val projectList =
        project.getExpressions.asScala
          .map(expr => expr.accept(expressionConverter))
          .map(toNamedExpression)
      if (createProject) {
        Project(projectList, child)
      } else {
        val aggregate: Aggregate = child.asInstanceOf[Aggregate]
        aggregate.copy(aggregateExpressions = projectList)
      }
    }
  }

  override def visit(expand: relation.Expand): LogicalPlan = {
    val child = expand.getInput.accept(this)
    val names = expand.getHint.get().getOutputNames.asScala

    withChild(child) {
      val projections = expand.getFields.asScala
        .map {
          case sf: SwitchingField =>
            sf.getDuplicates.asScala
              .map(expr => expr.accept(expressionConverter))
              .map(toNamedExpression)
          case _: ConsistentField =>
            throw new UnsupportedOperationException("ConsistentField not currently supported")
        }

      // An output column is nullable if any of the projections can assign null to it
      val types = projections.transpose.map(p => (p.head.dataType, p.exists(_.nullable)))

      val output = types
        .zip(names)
        .map { case (t, name) => StructField(name, t._1, t._2) }
        .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

      Expand(projections, output, child)
    }
  }

  override def visit(filter: relation.Filter): LogicalPlan = {
    val child = filter.getInput.accept(this)
    withChild(child) {
      val condition = filter.getCondition.accept(expressionConverter)
      Filter(condition, child)
    }
  }

  override def visit(set: relation.Set): LogicalPlan = {
    val children = set.getInputs.asScala.map(_.accept(this))
    withOutput(children.flatMap(_.output)) {
      set.getSetOp match {
        case SetOp.UNION_ALL => Union(children, byName = false, allowMissingCol = false)
        case op =>
          throw new UnsupportedOperationException(s"Operation not currently supported: $op")
      }
    }
  }

  override def visit(emptyScan: relation.EmptyScan): LogicalPlan = {
    LocalRelation(ToSubstraitType.toAttributeSeq(emptyScan.getInitialSchema))
  }

  override def visit(virtualTableScan: relation.VirtualTableScan): LogicalPlan = {
    val rows = virtualTableScan.getRows.asScala.map(
      row =>
        InternalRow.fromSeq(
          row
            .fields()
            .asScala
            .map(field => field.accept(expressionConverter).asInstanceOf[Literal].value)))
    LocalRelation(ToSubstraitType.toAttributeSeq(virtualTableScan.getInitialSchema), rows)
  }

  override def visit(namedScan: relation.NamedScan): LogicalPlan = {
    resolve(UnresolvedRelation(namedScan.getNames.asScala)) match {
      case m: MultiInstanceRelation => m.newInstance()
      case other => other
    }
  }

  override def visit(localFiles: LocalFiles): LogicalPlan = {
    val schema = ToSubstraitType.toStructType(localFiles.getInitialSchema)
    val output = schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    new LogicalRelation(
      relation = HadoopFsRelation(
        location = new InMemoryFileIndex(
          spark,
          localFiles.getItems.asScala.map(i => new Path(i.getPath.get())),
          Map(),
          Some(schema)),
        partitionSchema = new StructType(),
        dataSchema = schema,
        bucketSpec = None,
        fileFormat = new CSVFileFormat(),
        options = Map()
      )(spark),
      output = output,
      catalogTable = None,
      isStreaming = false
    )
  }

  private def withChild(child: LogicalPlan*)(body: => LogicalPlan): LogicalPlan = {
    val output = child.flatMap(_.output)
    withOutput(output)(body)
  }

  private def withOutput(output: Seq[NamedExpression])(body: => LogicalPlan): LogicalPlan = {
    expressionConverter.pushOutput(output)
    try {
      body
    } finally {
      expressionConverter.popOutput()
    }
  }
  private def resolve(plan: LogicalPlan): LogicalPlan = {
    val qe = new QueryExecution(spark, plan)
    qe.analyzed match {
      case SubqueryAlias(_, child) => child
      case other => other
    }
  }

  def convert(plan: Plan): LogicalPlan = {
    val root = plan.getRoots.get(0)
    val names = root.getNames.asScala
    val output = names.map(name => AttributeReference(name, DataTypes.StringType)())
    withOutput(output) {
      val logicalPlan = root.getInput.accept(this);
      val projectList: List[NamedExpression] = logicalPlan.output.zipWithIndex
        .map(
          z => {
            val (e, i) = z;
            if (e.name == names(i)) {
              e
            } else {
              Alias(e, names(i))()
            }
          })
        .toList
      val wrapper = Project(projectList, logicalPlan)
      require(wrapper.resolved)
      wrapper
    }
  }
}
