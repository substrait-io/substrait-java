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
import io.substrait.spark.compat.SparkCompat
import io.substrait.spark.expression._

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{caseSensitiveResolution, MultiInstanceRelation, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateTableCommand, DataWritingCommand, DropTableCommand, LeafRunnableCommand}
import org.apache.spark.sql.execution.datasources.{FileFormat => SparkFileFormat, FileIndex, InsertIntoHadoopFsRelationCommand, PartitionDirectory, V1Writes}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StructField, StructType}

import io.substrait.`type`.{NamedStruct, StringTypeVisitor, Type}
import io.substrait.{expression => exp}
import io.substrait.expression.{Expression => SExpression}
import io.substrait.plan.Plan
import io.substrait.relation
import io.substrait.relation.{ExtensionWrite, LocalFiles, NamedDdl, NamedWrite}
import io.substrait.relation.AbstractDdlRel.{DdlObject, DdlOp}
import io.substrait.relation.AbstractWriteRel.{CreateMode, WriteOp}
import io.substrait.relation.Expand.{ConsistentField, SwitchingField}
import io.substrait.relation.Set.SetOp
import io.substrait.relation.files.FileFormat
import io.substrait.relation.files.FileOrFiles.PathType
import io.substrait.relation.physical.{BroadcastExchange, MultiBucketExchange, RoundRobinExchange, ScatterExchange, SingleBucketExchange}
import io.substrait.util.EmptyVisitationContext
import org.apache.hadoop.fs.Path

import java.net.{URI, URISyntaxException}
import java.util.Optional

import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * RelVisitor to convert Substrait Rel plan to [[LogicalPlan]]. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
class ToLogicalPlan(val spark: AnyRef = SparkCompat.instance.getOrCreateSparkSession())
  extends DefaultRelVisitor[LogicalPlan] {

  private val expressionConverter =
    new ToSparkExpression(ToScalarFunction(SparkExtension.SparkScalarFunctions), Some(this))

  private def fromMeasure(measure: relation.Aggregate.Measure): AggregateExpression = {
    // this functions is called in createParentwithChild
    val function = measure.getFunction
    var arguments = function
      .arguments()
      .asScala
      .zipWithIndex
      .map {
        case (arg, i) =>
          arg.accept(
            function.declaration(),
            i,
            expressionConverter,
            EmptyVisitationContext.INSTANCE)
      }
      .toSeq
    if (function.declaration.name == "count" && function.arguments.size == 0) {
      // HACK - count() needs to be rewritten as count(1)
      arguments = ArrayBuffer(Literal(1)).toSeq
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

    if (function.sort().asScala.exists(!_.direction().isPresent)) {
      throw new UnsupportedOperationException(
        "A sort field using a custom comparison function is not supported")
    }

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
        .toSeq

      val outputs = groupBy.map(toNamedExpression)
      val aggregateExpressions =
        aggregate.getMeasures.asScala.map(fromMeasure).map(toNamedExpression).toSeq
      val plan = Aggregate(groupBy, outputs ++ aggregateExpressions, child)
      remap(plan, aggregate.getRemap)
    }
  }

  override def visit(
      window: relation.ConsistentPartitionWindow,
      context: EmptyVisitationContext): LogicalPlan = {
    val child = window.getInput.accept(this, context)
    withChild(child) {
      val partitions = window.getPartitionExpressions.asScala
        .map(expr => expr.accept(expressionConverter, context))
        .toSeq
      val sortOrders = window.getSorts.asScala.map(toSortOrder).toSeq
      val windowExpressions = window.getWindowFunctions.asScala
        .map(
          func => {
            val arguments = func
              .arguments()
              .asScala
              .zipWithIndex
              .map {
                case (arg, i) =>
                  arg.accept(func.declaration(), i, expressionConverter, context)
              }
              .toSeq
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
        .map(toNamedExpression(_))
        .toSeq
      val plan = Window(windowExpressions, partitions, sortOrders, child)
      remap(plan, window.getRemap)
    }
  }

  @nowarn("cat=deprecation")
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
        case relation.Join.JoinType.LEFT_SEMI => LeftSemi
        case relation.Join.JoinType.LEFT_ANTI => LeftAnti
        case relation.Join.JoinType.UNKNOWN =>
          throw new UnsupportedOperationException("Unknown join type is not supported")
        case other =>
          throw new UnsupportedOperationException(s"Unsupported join type $other")
      }
      val plan = Join(left, right, joinType, condition, hint = JoinHint.NONE)
      remap(plan, join.getRemap)
    }
  }

  override def visit(join: relation.Cross, context: EmptyVisitationContext): LogicalPlan = {
    val left = join.getLeft.accept(this, context)
    val right = join.getRight.accept(this, context)
    withChild(left, right) {
      // TODO: Support different join types here when join types are added to cross rel for BNLJ
      // Currently, this will change both cross and inner join types to inner join
      val plan = Join(left, right, Inner, Option(null), hint = JoinHint.NONE)
      remap(plan, join.getRemap)
    }
  }

  private def toSortOrder(sortField: SExpression.SortField): SortOrder = {
    if (!sortField.direction().isPresent) {
      throw new UnsupportedOperationException(
        "A sort field using a custom comparison function is not supported")
    }
    val expression = sortField.expr().accept(expressionConverter, EmptyVisitationContext.INSTANCE)
    val (direction, nullOrdering) = sortField.direction().get() match {
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
    // Offset/count are expressions; an unset count means LIMIT ALL (-1) and an unset offset means 0.
    val limit = if (fetch.getCount.isPresent) asInt(fetch.getCount.get) else -1
    val offset = if (fetch.getOffset.isPresent) asInt(fetch.getOffset.get) else 0
    val toLiteral = (i: Int) => Literal(i, IntegerType)
    val plan = if (limit >= 0) {
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
    remap(plan, fetch.getRemap)
  }

  private def asInt(e: SExpression): Int = e match {
    case l: SExpression.I64Literal => l.value().toInt
    case l: SExpression.I32Literal => l.value().toInt
    case other =>
      throw new UnsupportedOperationException(s"Unsupported fetch offset/count expression: $other")
  }

  override def visit(sort: relation.Sort, context: EmptyVisitationContext): LogicalPlan = {
    val child = sort.getInput.accept(this, context)
    withChild(child) {
      val sortOrders = sort.getSortFields.asScala.map(toSortOrder).toSeq
      val plan = Sort(sortOrders, global = true, child)
      remap(plan, sort.getRemap)
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
        ToSparkType
          .toStructType(NamedStruct.of(rel.getHint.get.getOutputNames, rel.getRecordType))
          .fieldNames
          .toSeq)
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
      val projectExprs = {
        project.getExpressions.asScala
          .map(_.accept(expressionConverter, context))
          .toSeq
      }
      val projectList = if (names.size == projectExprs.size) {
        projectExprs.zip(names).map { case (expr, name) => Alias(expr, name)() }
      } else {
        projectExprs.map(toNamedExpression)
      }
      if (createProject) {
        val allExpressions = output.map(_.toAttribute) ++ projectList
        val remapped = if (project.getRemap.isPresent) {
          project.getRemap.get().indices().asScala.map(allExpressions(_)).toSeq
        } else {
          allExpressions
        }
        val named = if (names.size == remapped.size) {
          remapped.zip(names).map { case (expr, name) => Alias(expr, name)() }
        } else {
          remapped
        }
        Project(named, child)
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
      val projections = expand.getFields.asScala.map {
        case sf: SwitchingField =>
          sf.getDuplicates.asScala
            .map(expr => expr.accept(expressionConverter, context))
            .map(toNamedExpression)
            .toSeq
        case _: ConsistentField =>
          throw new UnsupportedOperationException("ConsistentField not currently supported")
      }.toSeq

      // An output column is nullable if any of the projections can assign null to it
      val output = projections
        .map(p => (p.head.dataType, p.exists(_.nullable)))
        .zip(names)
        .map { case (t, name) => StructField(name, t._1, t._2) }
        .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

      val plan = Expand(projections.transpose, output, child)
      remap(plan, expand.getRemap)
    }
  }

  override def visit(filter: relation.Filter, context: EmptyVisitationContext): LogicalPlan = {
    val child = filter.getInput.accept(this, context)
    withChild(child) {
      val condition = filter.getCondition.accept(expressionConverter, context)
      val plan = Filter(condition, child)
      remap(plan, filter.getRemap)
    }
  }

  override def visit(set: relation.Set, context: EmptyVisitationContext): LogicalPlan = {
    def finish(plan: LogicalPlan): LogicalPlan = {
      val remapped = remap(plan, set.getRemap)
      fieldNames(set) match {
        case Some(names) if names.size == remapped.output.size =>
          Project(
            remapped.output.zip(names).map { case (attribute, name) => Alias(attribute, name)() },
            remapped)
        case _ => remapped
      }
    }
    if (set.getSetOp == SetOp.UNION_ALL) {
      combinePartitionScans(set.getInputs.asScala.toSeq, context) match {
        case Some(plan) => return finish(plan)
        case None =>
      }
    }
    val children = set.getInputs.asScala.map(_.accept(this, context)).toSeq
    withOutput(children.flatMap(_.output)) {
      val plan = set.getSetOp match {
        case SetOp.UNION_ALL => Union(children, byName = false, allowMissingCol = false)
        case op =>
          throw new UnsupportedOperationException(s"Operation not currently supported: $op")
      }
      finish(plan)
    }
  }

  private def combinePartitionScans(
      inputs: Seq[relation.Rel],
      context: EmptyVisitationContext): Option[LogicalPlan] = {
    val branches = inputs.map {
      case project: relation.Project
          if project.getExpressions.asScala.forall(_.isInstanceOf[SExpression.Literal]) =>
        project.getInput match {
          case read: LocalFiles
              if read.getRemap.isEmpty && read.getFilter.isEmpty &&
                read.getProjection.isEmpty && read.getItems.asScala.forall(
                  item =>
                    item.pathType.orElse(
                      null) == PathType.URI_FILE && item.getPath.isPresent && item.getStart == 0) =>
            Some((project, read))
          case _ => None
        }
      case _ => None
    }
    if (branches.isEmpty || branches.exists(_.isEmpty)) return None
    val scans = branches.flatten
    val (firstProject, firstRead) = scans.head
    val types = firstProject.getExpressions.asScala.map(_.getType).toSeq
    if (
      !scans.forall {
        case (project, read) =>
          read.getInitialSchema == firstRead.getInitialSchema &&
          project.getRemap == firstProject.getRemap && fieldNames(project) == fieldNames(
            firstProject) &&
          project.getExpressions.asScala.map(_.getType).toSeq == types
      }
    ) return None
    val items = scans.flatMap(_._2.getItems.asScala)
    val formats = items.map(_.getFileFormat).distinct
    if (items.isEmpty || formats.size != 1 || formats.head.isEmpty) return None
    val dataSchema = ToSparkType.toStructType(firstRead.getInitialSchema)
    // Internal names must not overlap file columns: the project's emit supplies the final order.
    var prefix = "__substrait_partition_"
    while (dataSchema.fieldNames.exists(_.toLowerCase(java.util.Locale.ROOT).startsWith(prefix))) {
      prefix = "_" + prefix
    }
    val literals = scans.map {
      case (project, _) =>
        project.getExpressions.asScala
          .map(_.accept(expressionConverter, context).asInstanceOf[Literal])
          .toSeq
    }
    if (
      literals.head.exists(_.dataType match {
        case _: ArrayType | _: MapType | _: StructType => true
        case _ => false
      })
    ) return None
    val partitionType = StructType(literals.head.zipWithIndex.map {
      case (literal, index) =>
        StructField(s"$prefix$index", literal.dataType, types(index).nullable())
    })
    val paths = items.map(item => toFilePath(item.getPath.get()))
    val listing =
      SparkCompat.instance.createInMemoryFileIndex(spark, paths, Map(), Some(dataSchema))
    val files = listing.allFiles().map(file => file.getPath -> file).toMap
    if (!paths.forall(files.contains)) return None
    val partitions = scans.zip(literals).map {
      case ((_, read), values) =>
        SparkCompat.instance.createPartitionDirectory(
          InternalRow.fromSeq(values.map(_.value)),
          read.getItems.asScala.map(item => files(toFilePath(item.getPath.get()))).toSeq)
    }
    val index = new FileIndex {
      override def rootPaths: Seq[Path] = paths
      override def inputFiles: Array[String] = paths.map(_.toUri.toString).toArray
      override def refresh(): Unit = listing.refresh()
      override def sizeInBytes: Long = partitions.flatMap(_.files.map(_.getLen)).sum
      override def partitionSchema: StructType = partitionType
      override def listFiles(
          partitionFilters: Seq[Expression],
          dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
        if (partitionFilters.isEmpty) partitions
        else {
          val bound = partitionFilters.reduceLeft(And).transform {
            case attribute: AttributeReference =>
              BoundReference(
                partitionSchema.fieldIndex(attribute.name),
                attribute.dataType,
                attribute.nullable)
          }
          val predicate = Predicate.createInterpreted(bound)
          partitions.filter(partition => predicate.eval(partition.values))
        }
      }
    }
    val (format, options) = convertFileFormat(formats.head.get())
    val fsRelation = SparkCompat.instance.createHadoopFsRelation(
      spark,
      index,
      partitionType,
      dataSchema,
      None,
      format,
      options)
    val output = fsRelation.schema.map(
      field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())
    val scan = SparkCompat.instance.createLogicalRelation(fsRelation, output, None, false)
    val projected = remap(scan, firstProject.getRemap)
    val hintNames = fieldNames(firstProject)
    val expressionNames = hintNames
      .filter(_.size == literals.head.size)
      .getOrElse(literals.head.map(toPrettySQL))
    val appendedNames = dataSchema.fieldNames.toSeq ++ expressionNames
    val remappedNames = if (firstProject.getRemap.isPresent) {
      firstProject.getRemap.get().indices().asScala.map(appendedNames(_)).toSeq
    } else appendedNames
    val names = hintNames.filter(_.size == projected.output.size).getOrElse(remappedNames)
    Some(
      Project(
        projected.output.zip(names).map { case (attribute, name) => Alias(attribute, name)() },
        projected))
  }

  override def visit(
      virtualTableScan: relation.VirtualTableScan,
      context: EmptyVisitationContext): LogicalPlan = {
    val rows = virtualTableScan.getRows.asScala.map {
      nestedStruct =>
        InternalRow.fromSeq(
          nestedStruct.fields.asScala
            .map(expr => expr.accept(expressionConverter, context).asInstanceOf[Literal].value)
            .toSeq
        )
    }.toSeq
    val plan = virtualTableScan.getInitialSchema match {
      case ns: NamedStruct if ns.names().isEmpty && rows.length == 1 =>
        OneRowRelation()
      case _ =>
        LocalRelation(ToSparkType.toAttributeSeq(virtualTableScan.getInitialSchema), rows)
    }
    remap(plan, virtualTableScan.getRemap)
  }

  override def visit(
      namedScan: relation.NamedScan,
      context: EmptyVisitationContext): LogicalPlan = {
    val plan = resolve(UnresolvedRelation(namedScan.getNames.asScala.toSeq)) match {
      case m: MultiInstanceRelation => m.newInstance()
      case other => other
    }
    remap(plan, namedScan.getRemap)
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
    val location = SparkCompat.instance.createInMemoryFileIndex(
      spark,
      localFiles.getItems.asScala.map(i => toFilePath(i.getPath.get())).toSeq,
      Map(),
      Some(schema))
    val hadoopFsRelation = SparkCompat.instance.createHadoopFsRelation(
      spark,
      location,
      new StructType(),
      schema,
      None,
      format,
      options
    )
    val plan = SparkCompat.instance.createLogicalRelation(
      relation = hadoopFsRelation,
      output = output,
      catalogTable = None,
      isStreaming = false
    )
    remap(plan, localFiles.getRemap)
  }

  private def toFilePath(path: String): Path = {
    try {
      val uri = new URI(path)
      if (uri.getScheme == null) new Path(path)
      else new Path(uri.getScheme, uri.getAuthority, uri.getPath)
    } catch {
      // Preserve support for unescaped local paths, such as filenames containing spaces.
      case _: URISyntaxException => new Path(path)
    }
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
    val table = catalogTable(write.getNames.asScala.toSeq)
    val isHive =
      SparkCompat.instance.getConf(spark, StaticSQLConf.CATALOG_IMPLEMENTATION.key) match {
        case "hive" => true
        case _ => false
      }
    val plan = write.getOperation match {
      case WriteOp.CTAS =>
        withChild(child) {
          if (isHive) {
            CreateHiveTableAsSelectCommand(
              table,
              child,
              write.getTableSchema.names().asScala.toSeq,
              saveMode(write.getCreateMode)
            )
          } else {
            CreateDataSourceTableAsSelectCommand(
              table,
              saveMode(write.getCreateMode),
              child,
              write.getTableSchema.names().asScala.toSeq
            )
          }
        }
      case WriteOp.INSERT if isHive =>
        withChild(child) {
          InsertIntoHiveTable(
            catalogTable(
              write.getNames.asScala.toSeq,
              ToSparkType.toStructType(write.getTableSchema)),
            Map.empty,
            child,
            write.getCreateMode == CreateMode.REPLACE_IF_EXISTS,
            false,
            write.getTableSchema.names().asScala.toSeq
          )
        }
      case op => throw new UnsupportedOperationException(s"Write mode $op not supported")
    }
    remap(plan, write.getRemap)
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
    val table = catalogTable(Seq(name))

    val plan = withChild(child) {
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
          outputColumnNames = write.getTableSchema.names.asScala.toSeq
        ))
    }
    remap(plan, write.getRemap)
  }

  override def visit(ddl: NamedDdl, context: EmptyVisitationContext): LogicalPlan = {
    val table =
      catalogTable(ddl.getNames.asScala.toSeq, ToSparkType.toStructType(ddl.getTableSchema))

    val plan = (ddl.getOperation, ddl.getObject) match {
      case (DdlOp.CREATE, DdlObject.TABLE) => CreateTableCommand(table, false)
      case (DdlOp.DROP, DdlObject.TABLE) => DropTableCommand(table.identifier, false, false, false)
      case (DdlOp.DROP_IF_EXIST, DdlObject.TABLE) =>
        DropTableCommand(table.identifier, true, false, false)
      case op => throw new UnsupportedOperationException(s"Ddl operation $op not supported")
    }
    remap(plan, ddl.getRemap)
  }

  private def catalogTable(
      names: Seq[String],
      schema: StructType = new StructType()): CatalogTable = {
    val (table, database, catalog) = names match {
      case Seq(table) => (table, None, None)
      case Seq(database, table) => (table, Some(database), None)
      case Seq(catalog, database, table) => (table, Some(database), Some(catalog))
      case names =>
        throw new UnsupportedOperationException(
          s"NamedWrite requires up to three names ([[catalog,] database,] table): $names")
    }

    val loc = SparkCompat.instance.getConf(spark, StaticSQLConf.WAREHOUSE_PATH.key)
    val storage = CatalogStorageFormat(
      locationUri = Some(URI.create(f"$loc/$table")),
      inputFormat = Some("org.apache.hadoop.mapred.TextInputFormat"),
      outputFormat = Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
      serde = None,
      compressed = false,
      properties = Map.empty
    )
    val id = TableIdentifier(table, database, catalog)
    CatalogTable(
      id,
      CatalogTableType.MANAGED,
      storage,
      schema,
      Some("parquet")
    )
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

  private def remap(plan: LogicalPlan, remap: Optional[relation.Rel.Remap]): LogicalPlan = {
    if (remap.isEmpty) {
      return plan
    }
    val projectExprs =
      plan.output.map { case ne: NamedExpression => ne.toAttribute }.map(toNamedExpression)
    Project(remap.get().indices().asScala.map(i => projectExprs(i)).toSeq, plan)
  }

  private def resolve(plan: LogicalPlan): LogicalPlan = {
    val qe = SparkCompat.instance.createQueryExecution(spark, plan)
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

  override def visit(exchange: ScatterExchange, context: EmptyVisitationContext): LogicalPlan = {
    visitFallback(exchange, context)
  }

  override def visit(
      exchange: SingleBucketExchange,
      context: EmptyVisitationContext): LogicalPlan = {
    visitFallback(exchange, context)
  }

  override def visit(
      exchange: MultiBucketExchange,
      context: EmptyVisitationContext): LogicalPlan = {
    visitFallback(exchange, context)
  }

  override def visit(exchange: RoundRobinExchange, context: EmptyVisitationContext): LogicalPlan = {
    visitFallback(exchange, context)
  }

  override def visit(exchange: BroadcastExchange, context: EmptyVisitationContext): LogicalPlan = {
    visitFallback(exchange, context)
  }
}
