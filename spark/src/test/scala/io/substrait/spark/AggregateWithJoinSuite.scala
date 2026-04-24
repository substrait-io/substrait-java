package io.substrait.spark

import io.substrait.dsl.SubstraitBuilder
import io.substrait.expression.{Expression, ExpressionCreator}
import io.substrait.extension.DefaultExtensionCatalog
import io.substrait.plan.Plan
import io.substrait.relation.{Aggregate, Join, Project, Rel, VirtualTableScan}
import io.substrait.spark.logical.ToLogicalPlan
import io.substrait.`type`.{NamedStruct, Type, TypeCreator}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession

import java.util
import java.util.Arrays
import scala.jdk.CollectionConverters._

/**
 * Test case that constructs a Substrait plan with aggregate operations using the builder syntax.
 * This test creates a plan equivalent to substrait_plan_with_aggregate_op.json but uses
 * VirtualTableScan instead of NamedTable for input data.
 *
 * The plan structure is:
 * - Root with output mapping [3, 4, 5] and names ["fuel_type", "postcode_area", "total_test_mileage"]
 * - Project with output mapping [3, 4, 5] (selecting fields 0, 1, 2 from aggregate)
 * - Aggregate grouping by fields 4 and 14, with sum(field 13)
 * - Project with output mapping [15-29] (selecting specific fields from join)
 * - Inner Join on tests.vehicle_id = vehicles.vehicle_id
 * - Left: Project selecting all 8 fields from tests VirtualTableScan
 * - Right: Project selecting all 7 fields from vehicles VirtualTableScan
 */
class AggregateWithJoinSuite extends SparkFunSuite with SharedSparkSession with SubstraitPlanTestBase {

  private val R = TypeCreator.REQUIRED
  private val N = TypeCreator.NULLABLE
  private val extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION
  private val sb = new SubstraitBuilder(extensions)

  private def createTestsRow(
      testId: Int,
      vehicleId: Int,
      testDate: Int,
      testClass: Int,
      testType: String,
      testResult: String,
      testMileage: Int,
      postcodeArea: String): Expression.NestedStruct = {
    Expression.NestedStruct.builder()
      .addFields(ExpressionCreator.i32(true, testId))
      .addFields(ExpressionCreator.i32(true, vehicleId))
      .addFields(ExpressionCreator.date(true, testDate))
      .addFields(ExpressionCreator.i32(true, testClass))
      .addFields(ExpressionCreator.string(true, testType))
      .addFields(ExpressionCreator.string(true, testResult))
      .addFields(ExpressionCreator.i32(true, testMileage))
      .addFields(ExpressionCreator.string(true, postcodeArea))
      .build()
  }

  /**
   * Creates a VirtualTableScan for the tests table with schema:
   * test_id (i32), vehicle_id (i32), test_date (date), test_class (i32),
   * test_type (varchar), test_result (varchar), test_mileage (i32), postcode_area (varchar)
   */
  private def createTestsTable(): VirtualTableScan = {
    val columnNames = Arrays.asList(
      "test_id", "vehicle_id", "test_date", "test_class",
      "test_type", "test_result", "test_mileage", "postcode_area")

    val struct = Type.Struct.builder()
      .addFields(N.I32)           // test_id
      .addFields(N.I32)           // vehicle_id
      .addFields(N.DATE)          // test_date
      .addFields(N.I32)           // test_class
      .addFields(N.STRING)        // test_type
      .addFields(N.STRING)        // test_result
      .addFields(N.I32)           // test_mileage
      .addFields(N.STRING)        // postcode_area
      .nullable(false)
      .build()

    val namedStruct = NamedStruct.of(columnNames, struct)

    VirtualTableScan.builder()
      .initialSchema(namedStruct)
      .addRows(createTestsRow(151, 100, 19000, 4, "MOT", "PASS", 50000, "GU"))
      .addRows(createTestsRow(385, 102, 20000, 4, "MOT", "PASS", 15000, "GU"))
      .addRows(createTestsRow(222, 101, 20000, 4, "MOT", "PASS", 20000, "PO"))
      .addRows(createTestsRow(164, 101, 20000, 4, "MOT", "FAIL", 35000, "GU"))
      .build()
  }

  private def createVehiclesRow(
      vehicleId: Int,
      make: String,
      model: String,
      colour: String,
      fuelType: String,
      cylinderCapacity: Int,
      firstUseDate: Int): Expression.NestedStruct = {
    Expression.NestedStruct.builder()
      .addFields(ExpressionCreator.i32(true, vehicleId))
      .addFields(ExpressionCreator.string(true, make))
      .addFields(ExpressionCreator.string(true, model))
      .addFields(ExpressionCreator.string(true, colour))
      .addFields(ExpressionCreator.string(true, fuelType))
      .addFields(ExpressionCreator.i32(true, cylinderCapacity))
      .addFields(ExpressionCreator.date(true, firstUseDate))
      .build()
  }

  /**
   * Creates a VirtualTableScan for the vehicles table with schema:
   * vehicle_id (i32), make (varchar), model (varchar), colour (varchar),
   * fuel_type (varchar), cylinder_capacity (i32), first_use_date (date)
   */
  private def createVehiclesTable(): VirtualTableScan = {
    val columnNames = Arrays.asList(
      "vehicle_id", "make", "model", "colour",
      "fuel_type", "cylinder_capacity", "first_use_date")

    val struct = Type.Struct.builder()
      .addFields(N.I32)           // vehicle_id
      .addFields(N.STRING)        // make
      .addFields(N.STRING)        // model
      .addFields(N.STRING)        // colour
      .addFields(N.STRING)        // fuel_type
      .addFields(N.I32)           // cylinder_capacity
      .addFields(N.DATE)          // first_use_date
      .nullable(false)
      .build()

    val namedStruct = NamedStruct.of(columnNames, struct)

    VirtualTableScan.builder()
      .initialSchema(namedStruct)
      .addRows(createVehiclesRow(100, "Ford", "Focus", "Blue", "Petrol", 1600, 18000))
      .addRows(createVehiclesRow(101, "VW", "Golf", "Red", "Diesel", 2000, 19000))
      .addRows(createVehiclesRow(100, "Ford", "Fiesta", "White", "Petrol", 1200, 18000))
      .build()
  }

  test("aggregate with join plan structure") {
    // Create base tables
    val testsTable = createTestsTable()
    val vehiclesTable = createVehiclesTable()

    // Project all fields from tests table with output mapping [8-15]
    val leftProject = Project.builder()
      .input(testsTable)
      .addExpressions(sb.fieldReference(testsTable, 0))  // test_id
      .addExpressions(sb.fieldReference(testsTable, 1))  // vehicle_id
      .addExpressions(sb.fieldReference(testsTable, 2))  // test_date
      .addExpressions(sb.fieldReference(testsTable, 3))  // test_class
      .addExpressions(sb.fieldReference(testsTable, 4))  // test_type
      .addExpressions(sb.fieldReference(testsTable, 5))  // test_result
      .addExpressions(sb.fieldReference(testsTable, 6))  // test_mileage
      .addExpressions(sb.fieldReference(testsTable, 7))  // postcode_area
      .remap(Rel.Remap.of(Arrays.asList(8, 9, 10, 11, 12, 13, 14, 15)))
      .build()

    // Project all fields from vehicles table with output mapping [7-13]
    val rightProject = Project.builder()
      .input(vehiclesTable)
      .addExpressions(sb.fieldReference(vehiclesTable, 0))  // vehicle_id
      .addExpressions(sb.fieldReference(vehiclesTable, 1))  // make
      .addExpressions(sb.fieldReference(vehiclesTable, 2))  // model
      .addExpressions(sb.fieldReference(vehiclesTable, 3))  // colour
      .addExpressions(sb.fieldReference(vehiclesTable, 4))  // fuel_type
      .addExpressions(sb.fieldReference(vehiclesTable, 5))  // cylinder_capacity
      .addExpressions(sb.fieldReference(vehiclesTable, 6))  // first_use_date
      .remap(Rel.Remap.of(Arrays.asList(7, 8, 9, 10, 11, 12, 13)))
      .build()

    // Inner join on tests.vehicle_id = vehicles.vehicle_id
    val join = sb.innerJoin(
      (joinInput: SubstraitBuilder.JoinInput) => {
        sb.equal(
          sb.fieldReference(joinInput, 1),   // tests.vehicle_id (field 1 from left)
          sb.fieldReference(joinInput, 8))   // vehicles.vehicle_id (field 8 in combined output)
      },
      leftProject,
      rightProject
    )

    // Project after join with output mapping [15-29]
    // Reorders to: vehicles fields (8-14) + tests fields (0-7)
    val postJoinProject = Project.builder()
      .input(join)
      .addExpressions(sb.fieldReference(join, 8))   // vehicles.vehicle_id
      .addExpressions(sb.fieldReference(join, 9))   // vehicles.make
      .addExpressions(sb.fieldReference(join, 10))  // vehicles.model
      .addExpressions(sb.fieldReference(join, 11))  // vehicles.colour
      .addExpressions(sb.fieldReference(join, 12))  // vehicles.fuel_type
      .addExpressions(sb.fieldReference(join, 13))  // vehicles.cylinder_capacity
      .addExpressions(sb.fieldReference(join, 14))  // vehicles.first_use_date
      .addExpressions(sb.fieldReference(join, 0))   // tests.test_id
      .addExpressions(sb.fieldReference(join, 1))   // tests.vehicle_id
      .addExpressions(sb.fieldReference(join, 2))   // tests.test_date
      .addExpressions(sb.fieldReference(join, 3))   // tests.test_class
      .addExpressions(sb.fieldReference(join, 4))   // tests.test_type
      .addExpressions(sb.fieldReference(join, 5))   // tests.test_result
      .addExpressions(sb.fieldReference(join, 6))   // tests.test_mileage
      .addExpressions(sb.fieldReference(join, 7))   // tests.postcode_area
      .remap(Rel.Remap.of(Arrays.asList(15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29)))
      .build()

    // Create aggregate with grouping by fuel_type (field 4) and postcode_area (field 14)
    // and sum of test_mileage (field 13)
    val aggregate = sb.aggregate(
      (input: Rel) => {
        // Create grouping by fuel_type and postcode_area
        Aggregate.Grouping.builder()
          .addExpressions(sb.fieldReference(input, 4))   // fuel_type (field 4 in postJoinProject)
          .addExpressions(sb.fieldReference(input, 14))  // postcode_area (field 14 in postJoinProject)
          .build()
      },
      (input: Rel) => {
        // Create measure for sum of test_mileage
        val sumMeasure = sb.sum(input, 13)  // sum of test_mileage (field 13 in postJoinProject)
        util.Arrays.asList(sumMeasure)
      },
      postJoinProject
    )

    // Project to select final output fields with output mapping [3, 4, 5]
    val finalProject = Project.builder()
      .input(aggregate)
      .addExpressions(sb.fieldReference(aggregate, 0))  // fuel_type (grouping key 0)
      .addExpressions(sb.fieldReference(aggregate, 1))  // postcode_area (grouping key 1)
      .addExpressions(sb.fieldReference(aggregate, 2))  // total_test_mileage (measure 0)
      .remap(Rel.Remap.of(Arrays.asList(3, 4, 5)))
      .build()

    // Wrap in a Plan with Root and output column names
    val root = Plan.Root.builder()
      .input(finalProject)
      .addNames("fuel_type", "postcode_area", "total_test_mileage")
      .build()

    val plan = Plan.builder()
      .addRoots(root)
      .build()

    val converter = new ToLogicalPlan(spark)
    val sparkPlan = converter.convert(plan)
    val output = DatasetUtil.fromLogicalPlan(spark, sparkPlan)
    // val output = spark.sessionState.executePlan(sparkPlan).executedPlan.execute()
    // there should be 1 row and 3 columns
    assertResult(3)(output.count())
    val rows = output.take(3)
    println(sparkPlan)

    // should produce the output:
    //    +---------+-------------+------------------+
    //    |fuel_type|postcode_area|total_test_mileage|
    //    +---------+-------------+------------------+
    //    |   Petrol|           GU|            100000|
    //    |   Diesel|           PO|             20000|
    //    |   Diesel|           GU|             35000|
    //    +---------+-------------+------------------+

    assertRow(rows(0), "Petrol", "GU", 100000)
    assertRow(rows(1), "Diesel", "PO", 20000)
    assertRow(rows(2), "Diesel", "GU", 35000)
  }

  def assertRow(row: Row, fuelType: String, postcodeArea: String, totalTestMileage: Long): Unit = {
    assertResult(fuelType)(row.getString(0))
    assertResult(postcodeArea)(row.getString(1))
    assertResult(totalTestMileage)(row.getLong(2))
  }
}
