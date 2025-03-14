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
package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}
import org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DataType
import io.substrait.debug.TreePrinter
import io.substrait.extension.ExtensionCollector
import io.substrait.plan.{Plan, PlanProtoConverter, ProtoPlanConverter}
import io.substrait.proto
import io.substrait.relation.{ProtoRelConverter, RelProtoConverter}
import org.scalactic.Equality
import org.scalactic.source.Position
import org.scalatest.Succeeded
import org.scalatest.compatible.Assertion
import org.scalatest.exceptions.{StackDepthException, TestFailedException}

trait SubstraitPlanTestBase { self: SharedSparkSession =>

  implicit class PlainEquality[T: TreePrinter](actual: T) {
    // Like should equal, but does not try to mark diffs in strings with square brackets,
    // so that IntelliJ can show a proper diff.
    def shouldEqualPlainly(expected: T)(implicit equality: Equality[T]): Assertion =
      if (!equality.areEqual(actual, expected)) {

        throw new TestFailedException(
          (e: StackDepthException) =>
            Some(
              s"${implicitly[TreePrinter[T]].tree(actual)}" +
                s" did not equal ${implicitly[TreePrinter[T]].tree(expected)}"),
          None,
          Position.here
        )
      } else Succeeded
  }

  def sqlToProtoPlan(sql: String): proto.Plan = {
    val convert = new ToSubstraitRel()
    val logicalPlan = plan(sql)
    val substraitRel = convert.visit(logicalPlan)

    val extensionCollector = new ExtensionCollector
    val relProtoConverter = new RelProtoConverter(extensionCollector)
    val builder = proto.Plan
      .newBuilder()
      .addRelations(
        proto.PlanRel
          .newBuilder()
          .setRoot(
            proto.RelRoot
              .newBuilder()
              .setInput(substraitRel
                .accept(relProtoConverter))
          )
      )
    extensionCollector.addExtensionsToPlan(builder)
    builder.build()
  }

  def assertProtoPlanRoundrip(sql: String): Plan = {
    val protoPlan1 = sqlToProtoPlan(sql)
    val plan = new ProtoPlanConverter().from(protoPlan1)
    val protoPlan2 = new PlanProtoConverter().toProto(plan)
    assertResult(protoPlan1)(protoPlan2)
    assertResult(1)(plan.getRoots.size())
    plan
  }

  def assertSqlSubstraitRelRoundTrip(query: String): LogicalPlan = {
    val sparkPlan = plan(query)


    // convert spark logical plan to substrait
    val substraitRel = new ToSubstraitRel().visit(sparkPlan)

    // Serialize to protobuf byte array
    val extensionCollector = new ExtensionCollector
    val bytes = new RelProtoConverter(extensionCollector).toProto(substraitRel).toByteArray

    // Read it back
    val protoPlan = io.substrait.proto.Rel.parseFrom(bytes)
    val substraitRel2 =
      new ProtoRelConverter(extensionCollector, SparkExtension.COLLECTION).from(protoPlan)

    // convert substrait back to spark plan
    val toLogicalPlan = new ToLogicalPlan(spark);
    val sparkPlan2 = substraitRel2.accept(toLogicalPlan)
    require(sparkPlan2.resolved)

    // and back to substrait again
    val substraitRel3 = new ToSubstraitRel().visit(sparkPlan2)

    // compare with original substrait plan to ensure it round-tripped (via proto bytes) correctly
    substraitRel3.shouldEqualPlainly(substraitRel)

    // Do one more roundtrip, this time with Substrait Plan object which contains also names,
    // to test that the Spark schemas match. This in some cases adds an extra Project
    // to rename fields, which then would break the round trip test we do above.
    val substraitPlan = new ToSubstraitRel().convert(sparkPlan)
    val sparkPlan3 = toLogicalPlan.convert(substraitPlan);
    require(sparkPlan3.resolved);

    assert(
      DataType.equalsStructurallyByName(
        sparkPlan.schema,
        sparkPlan3.schema,
        caseSensitiveResolution),
      "Expected: " + sparkPlan.schema + ", but got: " + sparkPlan3.schema
    )

    sparkPlan3
  }

  def plan(sql: String): LogicalPlan = {
    spark.sql(sql).queryExecution.optimizedPlan
  }

  def assertPlanRoundrip(plan: Plan): Unit = {
    val protoPlan1 = new PlanProtoConverter().toProto(plan)
    val protoPlan2 = new PlanProtoConverter().toProto(new ProtoPlanConverter().from(protoPlan1))
    assertResult(protoPlan1)(protoPlan2)
  }

  def testQuery(group: String, query: String, suffix: String = ""): Unit = {
    val queryString = resourceToString(
      s"$group/$query.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    assert(queryString != null)
    assertSqlSubstraitRelRoundTrip(queryString)
  }
}
