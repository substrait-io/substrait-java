/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.substrait.spark

import io.substrait.spark.utils.{Dialect, DialectGenerator}
import io.substrait.spark.utils.DialectGenerator.schemaPath

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.networknt.schema.{InputFormat, JsonSchemaFactory, SchemaValidatorsConfig, SpecVersion}

import java.io.{File, FileInputStream}

import scala.io.Source

class DialectSuite extends SparkFunSuite with SharedSparkSession with SubstraitPlanTestBase {
  private val dialectPath = "spark_dialect.yaml"

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  test("validate published dialect") {
    val jsonSchemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
    val config = SchemaValidatorsConfig.builder.build
    val schema = jsonSchemaFactory.getSchema(
      new FileInputStream(new File(schemaPath)),
      InputFormat.YAML,
      config)
    val dialect = Source.fromFile(dialectPath).mkString
    val errors = schema.validate(dialect, InputFormat.YAML)
    assertResult(java.util.Set.of())(errors)
  }

  test("generate validated YAML") {
    val tempPathName = "build/tmp/test/dialect.yaml"
    val tempFile = new File(tempPathName)
    if (tempFile.exists()) {
      tempFile.delete()
    }
    DialectGenerator.main(Array(tempPathName))
    assertResult(true)(tempFile.exists())
  }

  test("compare generated dialect") {
    val mapper = new ObjectMapper(new YAMLFactory()).registerModules(DefaultScalaModule)

    val genDialect = DialectGenerator.generate()
    val publishedDialect =
      mapper.readValue(new File(dialectPath), scala.reflect.classTag[Dialect].runtimeClass)
    // The following will fail if the generated dialect differs from the published one.
    // If this is caused by an intentional change, the published dialect should be regenerated using:
    // `./gradlew dialect`
    assertResult(publishedDialect)(genDialect)
  }

}
