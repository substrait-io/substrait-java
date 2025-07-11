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

import io.substrait.spark.{SparkExtension, ToSubstraitType}
import io.substrait.spark.expression._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Sum}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.{NullType, StructType}

import io.substrait.`type`.{NamedStruct, Type}
import io.substrait.{proto, relation}
import io.substrait.debug.TreePrinter
import io.substrait.expression.{Expression => SExpression, ExpressionCreator}
import io.substrait.extension.ExtensionCollector
import io.substrait.hint.Hint
import io.substrait.plan.Plan
import io.substrait.relation.RelProtoConverter
import io.substrait.relation.Set.SetOp
import io.substrait.relation.files.{FileFormat, FileOrFiles}
import io.substrait.relation.files.FileOrFiles.PathType
import io.substrait.util.EmptyVisitationContext
import io.substrait.utils.Util

import java.util
import java.util.{Collections, Optional}

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ToSubstraitRel extends AbstractLogicalPlanVisitor with Logging {

  private val toSubstraitExp = new WithLogicalSubQuery(this)

  private val TRUE = ExpressionCreator.bool(false, true)

  private val existenceJoins = scala.collection.mutable.Map[Long, SExpression.InPredicate]()

  def getExistenceJoin(id: Long): Option[SExpression.InPredicate] = existenceJoins.get(id)

  override def default(p: LogicalPlan): relation.Rel = p match {
    case p: LeafNode => convertReadOperator(p)
    case s: SubqueryAlias => visit(s.child)
    case other => t(other)
  }

  private def fromGroupSet(
      e: Seq[Expression],
      output: Seq[Attribute]): relation.Aggregate.Grouping = {

    relation.Aggregate.Grouping.builder
      .addAllExpressions(e.map(toExpression(output)).asJava)
      .build()
  }

  private def fromAggCall(
      expression: AggregateExpression,
      output: Seq[Attribute]): relation.Aggregate.Measure = {
    val substraitExps = expression.aggregateFunction.children.map(toExpression(output))
    val invocation =
      SparkExtension.toAggregateFunction.apply(expression, substraitExps)
    val filter = expression.filter.map(toExpression(output))
    relation.Aggregate.Measure.builder
      .function(invocation)
      .preMeasureFilter(Optional.ofNullable(filter.orNull))
      .build()
  }

  private def collectAggregates(
      resultExpressions: Seq[NamedExpression],
      aggExprToOutputOrdinal: mutable.HashMap[Expression, Int]): Seq[AggregateExpression] = {
    var ordinal = 0
    resultExpressions.flatMap {
      expr =>
        expr.collect {
          // Do not push down duplicated aggregate expressions. For example,
          // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
          // `max(a)` to the data source.
          case agg: AggregateExpression if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
            aggExprToOutputOrdinal(agg.canonicalized) = ordinal
            ordinal += 1
            agg
        }
    }
  }

  private def translateAggregation(
      groupBy: Seq[Expression],
      aggregates: Seq[AggregateExpression],
      output: Seq[Attribute],
      input: relation.Rel): relation.Aggregate = {
    val groupings = Collections.singletonList(fromGroupSet(groupBy, output))
    val aggCalls = aggregates.map(fromAggCall(_, output)).asJava

    relation.Aggregate.builder
      .input(input)
      .addAllGroupings(groupings)
      .addAllMeasures(aggCalls)
      .build
  }

  /**
   * The current substrait [[relation.Aggregate]] can't specify output, but spark [[Aggregate]]
   * allow. So To support #1 <code>select max(b) from table group by a</code>, and #2 <code>select
   * a, max(b) + 1 from table group by a</code>, We need create [[Project]] on top of [[Aggregate]]
   * to correctly support it.
   *
   * TODO: support [[Rollup]] and [[GroupingSets]]
   */
  override def visitAggregate(agg: Aggregate): relation.Rel = {
    val input = visit(agg.child)
    val actualResultExprs = agg.aggregateExpressions.map {
      // eliminate the internal MakeDecimal and UnscaledValue functions by undoing the spark optimisation:
      // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala#L2223
      case Alias(expr, name) =>
        Alias(
          expr.transform {
            case MakeDecimal(
                  ae @ AggregateExpression(Sum(UnscaledValue(e), _), _, _, _, _),
                  _,
                  _,
                  _) =>
              ae.copy(aggregateFunction = Sum(e))
            case Cast(
                  Divide(ae @ AggregateExpression(Average(UnscaledValue(e), _), _, _, _, _), _, _),
                  _,
                  _,
                  _) =>
              ae.copy(aggregateFunction = Average(e))
            case e => e
          },
          name
        )()
      case e => e
    }
    val actualGroupExprs = agg.groupingExpressions

    val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
    val aggregates = collectAggregates(actualResultExprs, aggExprToOutputOrdinal)
    val aggOutputMap = aggregates.zipWithIndex.map {
      case (e, i) =>
        AttributeReference(s"agg_func_$i", e.dataType, nullable = e.nullable)() -> e
    }
    val aggOutput = aggOutputMap.map(_._1)

    // collect group by
    val groupByExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
    actualGroupExprs.zipWithIndex.foreach {
      case (expr, ordinal) =>
        if (!groupByExprToOutputOrdinal.contains(expr.canonicalized)) {
          groupByExprToOutputOrdinal(expr.canonicalized) = ordinal
        }
    }
    val groupOutputMap = actualGroupExprs.zipWithIndex.map {
      case (e, i) =>
        AttributeReference(s"group_col_$i", e.dataType, nullable = e.nullable)() -> e
    }
    val groupOutput = groupOutputMap.map(_._1)

    val substraitAgg = translateAggregation(actualGroupExprs, aggregates, agg.child.output, input)
    val newOutput = groupOutput ++ aggOutput

    val projectExpressions = actualResultExprs.map {
      expr =>
        expr.transformDown {
          case agg: AggregateExpression =>
            val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
            aggOutput(ordinal)
          case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
            val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
            groupOutput(ordinal)
        }
    }
    val projects = projectExpressions.map(toExpression(newOutput))
    val names = ToSubstraitType.toNamedStruct(agg.schema).names()

    relation.Project.builder
      .remap(relation.Rel.Remap.offset(newOutput.size, projects.size))
      .expressions(projects.asJava)
      .hint(Hint.builder.addAllOutputNames(names).build())
      .input(substraitAgg)
      .build()
  }

  private def fromWindowCall(
      expression: WindowExpression,
      output: Seq[Attribute]): relation.ConsistentPartitionWindow.WindowRelFunctionInvocation = {
    val children = expression.windowFunction match {
      case agg: AggregateExpression => agg.aggregateFunction.children
      case _: RankLike => Seq.empty
      case other => other.children
    }
    val substraitExps = children.filter(_ != Literal(null, NullType)).map(toExpression(output))
    SparkExtension.toWindowFunction.apply(expression, substraitExps)
  }

  override def visitWindow(window: Window): relation.Rel = {
    val windowExpressions = window.windowExpressions.map {
      case w: WindowExpression => fromWindowCall(w, window.child.output)
      case Alias(w: WindowExpression, _) => fromWindowCall(w, window.child.output)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported window expression: $other")
    }.asJava

    val partitionExpressions = window.partitionSpec.map(toExpression(window.child.output)).asJava
    val sorts = window.orderSpec.map(toSortField(window.child.output)).asJava
    relation.ConsistentPartitionWindow.builder
      .input(visit(window.child))
      .addAllWindowFunctions(windowExpressions)
      .addAllPartitionExpressions(partitionExpressions)
      .addAllSorts(sorts)
      .build()
  }

  private def asLong(e: Expression): Long = e match {
    case IntegerLiteral(limit) => limit
    case other => throw new UnsupportedOperationException(s"Unknown type: $other")
  }

  private def fetch(child: LogicalPlan, offset: Long, limit: Long = -1): relation.Fetch = {
    val builder = relation.Fetch
      .builder()
      .input(visit(child))
      .offset(offset)
    if (limit != -1) {
      builder.count(limit)
    }

    builder.build()
  }

  override def visitGlobalLimit(p: GlobalLimit): relation.Rel = {
    p match {
      case OffsetAndLimit((offset, limit, child)) => fetch(child, offset, limit)
      case GlobalLimit(IntegerLiteral(globalLimit), LocalLimit(IntegerLiteral(localLimit), child))
          if globalLimit == localLimit =>
        fetch(child, 0, localLimit)
      case _ =>
        throw new UnsupportedOperationException(s"Unable to convert the limit expression: $p")
    }
  }

  override def visitLocalLimit(p: LocalLimit): relation.Rel = {
    val localLimit = asLong(p.limitExpr)
    p.child match {
      case OffsetAndLimit((offset, limit, child)) if localLimit >= limit =>
        fetch(child, offset, limit)
      case GlobalLimit(IntegerLiteral(globalLimit), child) if localLimit >= globalLimit =>
        fetch(child, 0, globalLimit)
      case _ => fetch(p.child, 0, localLimit)
    }
  }

  override def visitOffset(p: Offset): relation.Rel = {
    fetch(p.child, asLong(p.offsetExpr))
  }

  override def visitFilter(p: Filter): relation.Rel = {
    val input = visit(p.child)
    val condition = toExpression(p.child.output)(p.condition)
    relation.Filter.builder().condition(condition).input(input).build()
  }

  private def toSubstraitJoin(joinType: JoinType): relation.Join.JoinType = joinType match {
    case Inner | Cross => relation.Join.JoinType.INNER
    case LeftOuter => relation.Join.JoinType.LEFT
    case RightOuter => relation.Join.JoinType.RIGHT
    case FullOuter => relation.Join.JoinType.OUTER
    case LeftSemi => relation.Join.JoinType.LEFT_SEMI
    case LeftAnti => relation.Join.JoinType.LEFT_ANTI
    case other => throw new UnsupportedOperationException(s"Unsupported join type $other")
  }

  override def visitJoin(p: Join): relation.Rel = {
    p match {
      case Join(left, right, ExistenceJoin(exists), where, _) =>
        convertExistenceJoin(left, right, where, exists)
      case _ =>
        val left = visit(p.left)
        val right = visit(p.right)
        val condition =
          p.condition.map(toExpression(p.left.output ++ p.right.output)).getOrElse(TRUE)
        val joinType = toSubstraitJoin(p.joinType)

        if (joinType == relation.Join.JoinType.INNER && TRUE == condition) {
          relation.Cross.builder
            .left(left)
            .right(right)
            .build
        } else {
          relation.Join.builder
            .condition(condition)
            .joinType(joinType)
            .left(left)
            .right(right)
            .build
        }
    }
  }

  private def convertExistenceJoin(
      left: LogicalPlan,
      right: LogicalPlan,
      condition: Option[Expression],
      exists: Attribute): relation.Rel = {
    // An ExistenceJoin is an internal spark join type that is injected by the catalyst
    // optimiser. It doesn't directly map to any SQL join type, but can be modelled using
    // a Substrait `InPredicate` within a Filter condition.

    // The 'exists' attribute in the parent filter condition will be associated with this.

    // extract the needle expressions from the join condition
    def findNeedles(expr: Expression): Iterable[Expression] = expr match {
      case And(lhs, rhs) => findNeedles(lhs) ++ findNeedles(rhs)
      case EqualTo(lhs, _) => Seq(lhs)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unable to convert the ExistenceJoin condition: $expr")
    }
    val needles = condition.toIterable
      .flatMap(w => findNeedles(w))
      .map(toExpression(left.output))
      .asJava

    val haystack = visit(right)

    val inPredicate =
      SExpression.InPredicate.builder().needles(needles).haystack(haystack).build()
    // put this in a map exists->inPredicate for later lookup
    existenceJoins.put(exists.exprId.id, inPredicate)
    // return the left child, which will become the child of the enclosing filter
    visit(left)
  }

  override def visitProject(p: Project): relation.Rel = {
    val expressions = p.projectList.map(toExpression(p.child.output)).toList
    val child = visit(p.child)
    val remap = relation.Rel.Remap.offset(
      // when calculating the offset, ignore any placeholder 'exists' attributes
      p.child.output.count(o => !existenceJoins.contains(o.exprId.id)),
      expressions.size
    )

    relation.Project.builder
      .remap(remap)
      .expressions(expressions.asJava)
      .hint(Hint.builder.addAllOutputNames(ToSubstraitType.toNamedStruct(p.schema).names()).build())
      .input(child)
      .build()
  }

  override def visitExpand(p: Expand): relation.Rel = {
    val fields = p.projections.transpose.map(
      proj => {
        relation.Expand.SwitchingField.builder
          .duplicates(
            proj.map(toExpression(p.child.output)).asJava
          )
          .build()
      })

    relation.Expand.builder
      .fields(fields.asJava)
      .hint(Hint.builder.addAllOutputNames(ToSubstraitType.toNamedStruct(p.schema).names()).build())
      .input(visit(p.child))
      .build()
  }

  private def toSortField(output: Seq[Attribute] = Nil)(order: SortOrder): SExpression.SortField = {
    val direction = (order.direction, order.nullOrdering) match {
      case (Ascending, NullsFirst) => SExpression.SortDirection.ASC_NULLS_FIRST
      case (Descending, NullsFirst) => SExpression.SortDirection.DESC_NULLS_FIRST
      case (Ascending, NullsLast) => SExpression.SortDirection.ASC_NULLS_LAST
      case (Descending, NullsLast) => SExpression.SortDirection.DESC_NULLS_LAST
    }
    val expr = toExpression(output)(order.child)
    SExpression.SortField.builder().expr(expr).direction(direction).build()
  }
  override def visitSort(sort: Sort): relation.Rel = {
    val input = visit(sort.child)
    val fields = sort.order.map(toSortField(sort.child.output)).asJava
    relation.Sort.builder.addAllSortFields(fields).input(input).build
  }

  override def visitUnion(union: Union): relation.Rel = {
    if (union.byName) {
      throw new UnsupportedOperationException("Union by column name is not supported")
    }
    relation.Set.builder
      .inputs(union.children.map(c => visit(c)).asJava)
      .setOp(SetOp.UNION_ALL)
      .build()
  }

  private def toExpression(output: Seq[Attribute])(e: Expression): SExpression = {
    toSubstraitExp(e, output)
  }

  private def buildNamedScan(schema: StructType, tableNames: List[String]): relation.NamedScan = {
    val namedStruct = ToSubstraitType.toNamedStruct(schema)

    val namedScan = relation.NamedScan.builder
      .initialSchema(namedStruct)
      .addAllNames(tableNames.asJava)
      .build
    namedScan
  }
  private def buildVirtualTableScan(localRelation: LocalRelation): relation.AbstractReadRel = {
    val namedStruct = ToSubstraitType.toNamedStruct(localRelation.schema)

    if (localRelation.data.isEmpty) {
      relation.EmptyScan.builder().initialSchema(namedStruct).build()
    } else {
      relation.VirtualTableScan
        .builder()
        .initialSchema(namedStruct)
        .addAllRows(
          localRelation.data
            .map(
              row => {
                var idx = 0
                val buf = new ArrayBuffer[SExpression.Literal](row.numFields)
                while (idx < row.numFields) {
                  val dt = localRelation.schema(idx).dataType
                  val l = Literal.apply(row.get(idx, dt), dt)
                  buf += ToSubstraitLiteral.apply(l)
                  idx += 1
                }
                ExpressionCreator.struct(false, buf.asJava)
              })
            .asJava)
        .build()
    }
  }

  private def buildLocalFileScan(fsRelation: HadoopFsRelation): relation.AbstractReadRel = {
    val namedStruct = ToSubstraitType.toNamedStruct(fsRelation.schema)

    val format = fsRelation.fileFormat match {
      case _: CSVFileFormat =>
        // default values for options specified here:
        // https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option
        FileFormat.DelimiterSeparatedTextReadOptions
          .builder()
          .fieldDelimiter(fsRelation.options.getOrElse("delimiter", ","))
          .maxLineSize(0)
          .quote(fsRelation.options.getOrElse("quote", "\""))
          .headerLinesToSkip(if (fsRelation.options.getOrElse("header", false) == false) 0 else 1)
          .escape(fsRelation.options.getOrElse("escape", "\\"))
          .valueTreatedAsNull(Optional.ofNullable(fsRelation.options.get("nullValue").orNull))
          .build()
      case _: ParquetFileFormat => FileFormat.ParquetReadOptions.builder().build()
      case _: OrcFileFormat => FileFormat.OrcReadOptions.builder().build()
      case format =>
        throw new UnsupportedOperationException(s"File format not currently supported: $format")
    }

    relation.LocalFiles
      .builder()
      .initialSchema(namedStruct)
      .addAllItems(
        fsRelation.location.inputFiles
          .map(
            file => {
              FileOrFiles
                .builder()
                .fileFormat(format)
                .partitionIndex(0)
                .start(0)
                .length(fsRelation.sizeInBytes)
                .path(file)
                .pathType(PathType.URI_FILE)
                .build()
            })
          .toList
          .asJava
      )
      .build()
  }

  /** Read Operator: https://substrait.io/relations/logical_relations/#read-operator */
  private def convertReadOperator(plan: LeafNode): relation.AbstractReadRel = {
    var tableNames: List[String] = null
    plan match {
      case logicalRelation: LogicalRelation if logicalRelation.catalogTable.isDefined =>
        tableNames = logicalRelation.catalogTable.get.identifier.unquotedString.split("\\.").toList
        buildNamedScan(logicalRelation.schema, tableNames)
      case dataSourceV2ScanRelation: DataSourceV2ScanRelation =>
        tableNames = dataSourceV2ScanRelation.relation.identifier.get.toString.split("\\.").toList
        buildNamedScan(dataSourceV2ScanRelation.schema, tableNames)
      case dataSourceV2Relation: DataSourceV2Relation =>
        tableNames = dataSourceV2Relation.identifier.get.toString.split("\\.").toList
        buildNamedScan(dataSourceV2Relation.schema, tableNames)
      case hiveTableRelation: HiveTableRelation =>
        tableNames = hiveTableRelation.tableMeta.identifier.unquotedString.split("\\.").toList
        buildNamedScan(hiveTableRelation.schema, tableNames)
      case localRelation: LocalRelation => buildVirtualTableScan(localRelation)
      case logicalRelation: LogicalRelation =>
        logicalRelation.relation match {
          case fsRelation: HadoopFsRelation =>
            buildLocalFileScan(fsRelation)
          case _ =>
            throw new UnsupportedOperationException(
              s"******* Unable to convert the plan to a substrait relation: " +
                s"${logicalRelation.relation.toString}")
        }
      case _: OneRowRelation =>
        relation.VirtualTableScan
          .builder()
          .initialSchema(NamedStruct
            .of(new util.ArrayList[String](), Type.Struct.builder().nullable(false).build()))
          .addRows(ExpressionCreator.struct(false))
          .build()
      case _ =>
        throw new UnsupportedOperationException(
          s"******* Unable to convert the plan to a substrait NamedScan: $plan")
    }
  }
  def convert(p: LogicalPlan): Plan = {
    val rel = visit(p)
    Plan.builder
      .version(
        Plan.Version
          .builder()
          .from(Plan.Version.DEFAULT_VERSION)
          .producer("substrait-spark")
          .build())
      .roots(
        Collections.singletonList(
          Plan.Root
            .builder()
            .input(rel)
            .addAllNames(ToSubstraitType.toNamedStruct(p.schema).names())
            .build()
        ))
      .build()
  }

  def tree(p: LogicalPlan): String = {
    TreePrinter.tree(visit(p))
  }

  def toProtoSubstrait(p: LogicalPlan): Array[Byte] = {
    val substraitRel = visit(p)

    val extensionCollector = new ExtensionCollector
    val relProtoConverter = new RelProtoConverter(extensionCollector)
    val builder = proto.Plan
      .newBuilder()
      .addRelations(
        proto.PlanRel
          .newBuilder()
          .setRel(substraitRel
            .accept(relProtoConverter, EmptyVisitationContext.INSTANCE))
      )
    extensionCollector.addExtensionsToPlan(builder)
    builder.build().toByteArray
  }
}

private[logical] class WithLogicalSubQuery(toSubstraitRel: ToSubstraitRel)
  extends ToSubstraitExpression {
  override protected val toScalarFunction: ToScalarFunction =
    ToScalarFunction(SparkExtension.SparkScalarFunctions)

  override protected def translateSubQuery(expr: PlanExpression[_]): Option[SExpression] = {
    expr match {
      case s: ScalarSubquery if s.outerAttrs.isEmpty && s.joinCond.isEmpty =>
        val rel = toSubstraitRel.visit(s.plan)
        val t =
          s.plan.schema.fields.head // Using this instead of s.dataType/s.nullable to get correct nullability
        Some(
          SExpression.ScalarSubquery.builder
            .input(rel)
            .`type`(ToSubstraitType.apply(t.dataType, t.nullable))
            .build())
      case other => default(other)
    }
  }

  override protected def translateInSubquery(expr: InSubquery): Option[SExpression] = {
    Util
      .seqToOption(expr.values.map(translateUp).toList)
      .flatMap(
        values =>
          Some(
            SExpression.InPredicate
              .builder()
              .needles(values.asJava)
              .haystack(toSubstraitRel.visit(expr.query.plan))
              .build()
          ))
  }

  override protected def translateAttribute(a: AttributeReference): Option[SExpression] = {
    toSubstraitRel.getExistenceJoin(a.exprId.id) match {
      case Some(exists) => Some(exists)
      case None => super.translateAttribute(a)
    }
  }
}
