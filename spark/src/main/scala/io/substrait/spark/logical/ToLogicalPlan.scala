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

import io.substrait.spark.{DefaultRelVisitor, FileHolder, SparkExtension, ToSparkType, ToSubstraitType}
import io.substrait.spark.expression._

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{caseSensitiveResolution, MultiInstanceRelation, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, LeafRunnableCommand}
import org.apache.spark.sql.execution.datasources.{FileFormat => SparkFileFormat, HadoopFsRelation, InMemoryFileIndex, InsertIntoHadoopFsRelationCommand, LogicalRelation, V1Writes}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

import io.substrait.`type`.{NamedStruct, StringTypeVisitor, Type}
import io.substrait.{expression => exp}
import io.substrait.expression.{Expression => SExpression}
import io.substrait.plan.Plan
import io.substrait.relation
import io.substrait.relation.{ExtensionWrite, LocalFiles, NamedWrite}
import io.substrait.relation.AbstractWriteRel.{CreateMode, WriteOp}
import io.substrait.relation.Expand.{ConsistentField, SwitchingField}
import io.substrait.relation.Set.SetOp
import io.substrait.relation.files.FileFormat
import io.substrait.util.EmptyVisitationContext
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer

/**
 * RelVisitor to convert Substrait Rel plan to [[LogicalPlan]]. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
class ToLogicalPlan(spark: SparkSession = SparkSession.builder().getOrCreate())
  extends DefaultRelVisitor[LogicalPlan] {

  private val expressionConverter =
    new ToSparkExpression(ToScalarFunction(SparkExtension.SparkScalarFunctions), Some(this))

  private def fromMeasure(measure: relation.Aggregate.Measure): AggregateExpression = {
    // this functions is called in createParentwithChild
    val function = measure.getFunction
    var arguments = function.arguments().asScala.zipWithIndex.map {
      case (arg, i) =>
        arg.accept(function.declaration(), i, expressionConverter, EmptyVisitationContext.INSTANCE)
    }
    if (function.declaration.name == "count" && function.arguments.size == 0) {
      // HACK - count() needs to be rewritten as count(1)
      arguments = ArrayBuffer(Literal(1))
    }

    val aggregateFunction = SparkExtension.toAggregateFunction
      .getSparkExpressionFromSubstraitFunc(function.declaration.key, arguments)
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
      .map(_.accept(expressionConverter, EmptyVisitationContext.INSTANCE))

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

  override def visit(
      aggregate: relation.Aggregate,
      context: EmptyVisitationContext): LogicalPlan = {
    require(aggregate.getGroupings.size() == 1)
    val child = aggregate.getInput.accept(this, context)
    withChild(child) {
      val groupBy = aggregate.getGroupings
        .get(0)
        .getExpressions
        .asScala
        .map(expr => expr.accept(expressionConverter, context))

      val outputs = groupBy.map(toNamedExpression)
      val aggregateExpressions =
        aggregate.getMeasures.asScala.map(fromMeasure).map(toNamedExpression)
      Aggregate(groupBy, outputs ++= aggregateExpressions, child)
    }
  }

  override def visit(
      window: relation.ConsistentPartitionWindow,
      context: EmptyVisitationContext): LogicalPlan = {
    val child = window.getInput.accept(this, context)
    withChild(child) {
      val partitions = window.getPartitionExpressions.asScala
        .map(expr => expr.accept(expressionConverter, context))
      val sortOrders = window.getSorts.asScala.map(toSortOrder)
      val windowExpressions = window.getWindowFunctions.asScala
        .map(
          func => {
            val arguments = func.arguments().asScala.zipWithIndex.map {
              case (arg, i) =>
                arg.accept(func.declaration(), i, expressionConverter, context)
            }
            val windowFunction = SparkExtension.toWindowFunction
              .getSparkExpressionFromSubstraitFunc(func.declaration.key, arguments)
              .map {
                case win: WindowFunction => win
                case agg: AggregateFunction =>
                  AggregateExpression(
                    agg,
                    ToAggregateFunction.toSpark(func.aggregationPhase()),
                    ToAggregateFunction.toSpark(func.invocation()),
                    None)
              }
              .getOrElse({
                val msg = String.format(
                  "Unable to convert Window function %s(%s).",
                  func.declaration.name,
                  func.arguments.asScala
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
            val frame =
              ToWindowFunction.toSparkFrame(func.boundsType(), func.lowerBound(), func.upperBound())
            val spec = WindowSpecDefinition(partitions, sortOrders, frame)
            WindowExpression(windowFunction, spec)
          })
        .map(toNamedExpression)
      Window(windowExpressions, partitions, sortOrders, child)
    }
  }

  override def visit(join: relation.Join, context: EmptyVisitationContext): LogicalPlan = {
    val left = join.getLeft.accept(this, context)
    val right = join.getRight.accept(this, context)
    withChild(left, right) {
      val condition = Option(join.getCondition.orElse(null))
        .map(_.accept(expressionConverter, context))

      val joinType = join.getJoinType match {
        case relation.Join.JoinType.INNER => Inner
        case relation.Join.JoinType.LEFT => LeftOuter
        case relation.Join.JoinType.RIGHT => RightOuter
        case relation.Join.JoinType.OUTER => FullOuter
        case relation.Join.JoinType.SEMI => LeftSemi
        case relation.Join.JoinType.ANTI => LeftAnti
        case relation.Join.JoinType.LEFT_SEMI => LeftSemi
        case relation.Join.JoinType.LEFT_ANTI => LeftAnti
        case relation.Join.JoinType.UNKNOWN =>
          throw new UnsupportedOperationException("Unknown join type is not supported")
        case other =>
          throw new UnsupportedOperationException(s"Unsupported join type $other")
      }
      Join(left, right, joinType, condition, hint = JoinHint.NONE)
    }
  }

  override def visit(join: relation.Cross, context: EmptyVisitationContext): LogicalPlan = {
    val left = join.getLeft.accept(this, context)
    val right = join.getRight.accept(this, context)
    withChild(left, right) {
      // TODO: Support different join types here when join types are added to cross rel for BNLJ
      // Currently, this will change both cross and inner join types to inner join
      Join(left, right, Inner, Option(null), hint = JoinHint.NONE)
    }
  }

  private def toSortOrder(sortField: SExpression.SortField): SortOrder = {
    val expression = sortField.expr().accept(expressionConverter, EmptyVisitationContext.INSTANCE)
    val (direction, nullOrdering) = sortField.direction() match {
      case SExpression.SortDirection.ASC_NULLS_FIRST => (Ascending, NullsFirst)
      case SExpression.SortDirection.DESC_NULLS_FIRST => (Descending, NullsFirst)
      case SExpression.SortDirection.ASC_NULLS_LAST => (Ascending, NullsLast)
      case SExpression.SortDirection.DESC_NULLS_LAST => (Descending, NullsLast)
      case other =>
        throw new UnsupportedOperationException(
          s"Unexpected Expression.SortDirection enum: $other !")
    }
    SortOrder(expression, direction, nullOrdering, Seq.empty)
  }

  override def visit(fetch: relation.Fetch, context: EmptyVisitationContext): LogicalPlan = {
    val child = fetch.getInput.accept(this, context)
    val limit = fetch.getCount.orElse(-1).intValue() // -1 means unassigned here
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

  override def visit(sort: relation.Sort, context: EmptyVisitationContext): LogicalPlan = {
    val child = sort.getInput.accept(this, context)
    withChild(child) {
      val sortOrders = sort.getSortFields.asScala.map(toSortOrder)
      Sort(sortOrders, global = true, child)
    }
  }

  /**
   * Returns the top level field (column) names for the given relation, if they have been specified
   * in the optional `hint` message. Does not include the field names of any inner structs.
   * @param rel
   * @return
   *   Optional list of names.
   */
  private def fieldNames(rel: relation.Rel): Option[Seq[String]] = {
    if (rel.getHint.isPresent && !rel.getHint.get().getOutputNames.isEmpty) {
      Some(
        ToSubstraitType
          .toNamedStruct(ToSparkType.toStructType(
            NamedStruct.of(rel.getHint.get.getOutputNames, rel.getRecordType)))
          .names
          .asScala)
    } else {
      None
    }
  }

  override def visit(project: relation.Project, context: EmptyVisitationContext): LogicalPlan = {
    val child = project.getInput.accept(this, context)
    val (output, createProject) = child match {
      case a: Aggregate => (a.aggregateExpressions, false)
      case other => (other.output, true)
    }
    val names = fieldNames(project).getOrElse(List.empty)

    withOutput(output) {
      val projectExprs =
        project.getExpressions.asScala
          .map(expr => expr.accept(expressionConverter, context))
      val projectList = if (names.size == projectExprs.size) {
        projectExprs.zip(names).map { case (expr, name) => Alias(expr, name)() }
      } else {
        projectExprs.map(toNamedExpression)
      }
      if (createProject) {
        Project(projectList, child)
      } else {
        val aggregate: Aggregate = child.asInstanceOf[Aggregate]
        aggregate.copy(aggregateExpressions = projectList)
      }
    }
  }

  override def visit(expand: relation.Expand, context: EmptyVisitationContext): LogicalPlan = {
    val child = expand.getInput.accept(this, context)
    val names = fieldNames(expand).getOrElse(
      expand.getFields.asScala.zipWithIndex.map { case (_, i) => s"col$i" }
    )

    withChild(child) {
      val projections = expand.getFields.asScala
        .map {
          case sf: SwitchingField =>
            sf.getDuplicates.asScala
              .map(expr => expr.accept(expressionConverter, context))
              .map(toNamedExpression)
          case _: ConsistentField =>
            throw new UnsupportedOperationException("ConsistentField not currently supported")
        }

      // An output column is nullable if any of the projections can assign null to it
      val output = projections
        .map(p => (p.head.dataType, p.exists(_.nullable)))
        .zip(names)
        .map { case (t, name) => StructField(name, t._1, t._2) }
        .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

      Expand(projections.transpose, output, child)
    }
  }

  override def visit(filter: relation.Filter, context: EmptyVisitationContext): LogicalPlan = {
    val child = filter.getInput.accept(this, context)
    withChild(child) {
      val condition = filter.getCondition.accept(expressionConverter, context)
      Filter(condition, child)
    }
  }

  override def visit(set: relation.Set, context: EmptyVisitationContext): LogicalPlan = {
    val children = set.getInputs.asScala.map(_.accept(this, context))
    withOutput(children.flatMap(_.output)) {
      set.getSetOp match {
        case SetOp.UNION_ALL => Union(children, byName = false, allowMissingCol = false)
        case op =>
          throw new UnsupportedOperationException(s"Operation not currently supported: $op")
      }
    }
  }

  override def visit(
      emptyScan: relation.EmptyScan,
      context: EmptyVisitationContext): LogicalPlan = {
    LocalRelation(ToSparkType.toAttributeSeq(emptyScan.getInitialSchema))
  }

  override def visit(
      virtualTableScan: relation.VirtualTableScan,
      context: EmptyVisitationContext): LogicalPlan = {
    val rows = virtualTableScan.getRows.asScala.map(
      row =>
        InternalRow.fromSeq(
          row
            .fields()
            .asScala
            .map(field => field.accept(expressionConverter, context).asInstanceOf[Literal].value)))
    virtualTableScan.getInitialSchema match {
      case ns: NamedStruct if ns.names().isEmpty && rows.length == 1 =>
        OneRowRelation()
      case _ =>
        LocalRelation(ToSparkType.toAttributeSeq(virtualTableScan.getInitialSchema), rows)
    }
  }

  override def visit(
      namedScan: relation.NamedScan,
      context: EmptyVisitationContext): LogicalPlan = {
    resolve(UnresolvedRelation(namedScan.getNames.asScala)) match {
      case m: MultiInstanceRelation => m.newInstance()
      case other => other
    }
  }

  override def visit(localFiles: LocalFiles, context: EmptyVisitationContext): LogicalPlan = {
    val schema = ToSparkType.toStructType(localFiles.getInitialSchema)
    val output = schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

    // spark requires that all files have the same format
    val formats = localFiles.getItems.asScala.map(i => i.getFileFormat.orElse(null)).distinct
    if (formats.length != 1) {
      throw new UnsupportedOperationException(s"All files must have the same format")
    }
    val (format, options) = convertFileFormat(formats.head)
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
        fileFormat = format,
        options = options
      )(spark),
      output = output,
      catalogTable = None,
      isStreaming = false
    )
  }

  def convertFileFormat(fileFormat: FileFormat): (SparkFileFormat, Map[String, String]) = {
    fileFormat match {
      case csv: FileFormat.DelimiterSeparatedTextReadOptions =>
        val opts = scala.collection.mutable.Map[String, String](
          "delimiter" -> csv.getFieldDelimiter,
          "quote" -> csv.getQuote,
          "header" -> (csv.getHeaderLinesToSkip match {
            case 0 => "false"
            case 1 => "true"
            case _ =>
              throw new UnsupportedOperationException(
                s"Cannot configure CSV reader to skip ${csv.getHeaderLinesToSkip} rows")
          }),
          "escape" -> csv.getEscape
        )
        csv.getValueTreatedAsNull.ifPresent(nullValue => opts("nullValue") = nullValue)
        (new CSVFileFormat, opts.toMap)
      case _: FileFormat.ParquetReadOptions => (new ParquetFileFormat(), Map.empty[String, String])
      case _: FileFormat.OrcReadOptions => (new OrcFileFormat(), Map.empty[String, String])
      case format =>
        throw new UnsupportedOperationException(s"File format not currently supported: $format")
    }
  }

  override def visit(write: NamedWrite, context: EmptyVisitationContext): LogicalPlan = {
    val child = write.getInput.accept(this, context)

    val (table, database, catalog) = write.getNames.asScala match {
      case Seq(table) => (table, None, None)
      case Seq(database, table) => (table, Some(database), None)
      case Seq(catalog, database, table) => (table, Some(database), Some(catalog))
      case names =>
        throw new UnsupportedOperationException(
          s"NamedWrite requires up to three names ([[catalog,] database,] table): $names")
    }
    val id = TableIdentifier(table, database, catalog)
    val catalogTable = CatalogTable(
      id,
      CatalogTableType.MANAGED,
      CatalogStorageFormat.empty,
      new StructType(),
      Some("parquet")
    )
    write.getOperation match {
      case WriteOp.CTAS =>
        withChild(child) {
          CreateDataSourceTableAsSelectCommand(
            catalogTable,
            saveMode(write.getCreateMode),
            child,
            write.getTableSchema.names().asScala
          )
        }
      case op => throw new UnsupportedOperationException(s"Write mode $op not supported")
    }
  }

  override def visit(write: ExtensionWrite, context: EmptyVisitationContext): LogicalPlan = {
    val child = write.getInput.accept(this, context)
    val mode = write.getOperation match {
      case WriteOp.INSERT => SaveMode.Append
      case WriteOp.UPDATE => SaveMode.Overwrite
      case op => throw new UnsupportedOperationException(s"Write mode $op not supported")
    }

    val file = write.getDetail match {
      case FileHolder(f) => f
      case d =>
        throw new UnsupportedOperationException(s"Unsupported extension detail: ${d.getClass}")
    }

    if (file.getPath.isEmpty)
      throw new UnsupportedOperationException("The File extension detail must contain a Path field")
    if (file.getFileFormat.isEmpty)
      throw new UnsupportedOperationException(
        "The File extension detail must contain a FileFormat field")

    val (format, options) = convertFileFormat(file.getFileFormat.get)

    val name = file.getPath.get.split('/').reverse.head
    val id = TableIdentifier(name)
    val table = CatalogTable(
      id,
      CatalogTableType.MANAGED,
      CatalogStorageFormat.empty,
      new StructType(),
      None
    )

    withChild(child) {
      V1Writes.apply(
        InsertIntoHadoopFsRelationCommand(
          outputPath = new Path(file.getPath.get),
          staticPartitions = Map(),
          ifPartitionNotExists = false,
          partitionColumns = Seq.empty,
          bucketSpec = None,
          fileFormat = format,
          options = options,
          query = child,
          mode = mode,
          catalogTable = Some(table),
          fileIndex = None,
          outputColumnNames = write.getTableSchema.names.asScala
        ))
    }
  }

  private def saveMode(mode: CreateMode): SaveMode = mode match {
    case CreateMode.APPEND_IF_EXISTS => SaveMode.Append
    case CreateMode.REPLACE_IF_EXISTS => SaveMode.Overwrite
    case CreateMode.ERROR_IF_EXISTS => SaveMode.ErrorIfExists
    case CreateMode.IGNORE_IF_EXISTS => SaveMode.Ignore
    case _ => throw new UnsupportedOperationException(s"Unsupported mode: $mode")
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

  def convert(rel: relation.Rel): LogicalPlan = {
    val logicalPlan = rel.accept(this, EmptyVisitationContext.INSTANCE)
    require(logicalPlan.resolved)
    logicalPlan
  }

  def convert(plan: Plan): LogicalPlan = {
    require(plan.getRoots.size() == 1)
    val root = plan.getRoots.get(0)
    val logicalPlan = convert(root.getInput)

    // Substrait plans do not have column names within the plan, only at the leaf (ReadRel) level and root level.
    // So we need to do some mangling at the end to ensure the output schema is correct.
    // The final names in the root are given as a depth-first traversal of the schema, including inner struct fields
    val targetSchema =
      ToSparkType.toStructType(NamedStruct.of(root.getNames, root.getInput.getRecordType))

    // Short-circuit: if schema matches already, then we don't need to do anything
    if (
      DataType.equalsStructurallyByName(logicalPlan.schema, targetSchema, caseSensitiveResolution)
    ) {
      return logicalPlan
    }

    val renameAndCastExprs = (old: Seq[NamedExpression]) =>
      old.zip(targetSchema.fields).map {
        case (oldNamedExpr, targetField) =>
          if (
            !DataType.equalsStructurallyByName(
              oldNamedExpr.dataType,
              targetField.dataType,
              caseSensitiveResolution)
          ) {
            Alias(
              Cast(oldNamedExpr, targetField.dataType, Some(SQLConf.get.sessionLocalTimeZone)),
              targetField.name)()
          } else if (!oldNamedExpr.name.equals(targetField.name)) {
            Alias(oldNamedExpr, targetField.name)()
          } else {
            oldNamedExpr
          }
      }

    val renamedLogicalPlan = logicalPlan match {
      // If the plan ends in a relation that produces columns, we bake in the new names to that existing relation
      // This is helps a bit with round-trip testing and plan readability
      case project: Project => Project(renameAndCastExprs(project.projectList), project.child)
      case aggregate: Aggregate =>
        Aggregate(
          aggregate.groupingExpressions,
          renameAndCastExprs(aggregate.aggregateExpressions),
          aggregate.child)
      // if the plan represents a 'write' command, then leave as is
      case _: DataWritingCommand => logicalPlan
      case _: LeafRunnableCommand => logicalPlan
      // Otherwise we add a project to enforce correct names in the output
      case _ => Project(renameAndCastExprs(logicalPlan.output), logicalPlan)
    }

    require(renamedLogicalPlan.resolved)
    renamedLogicalPlan
  }
}
