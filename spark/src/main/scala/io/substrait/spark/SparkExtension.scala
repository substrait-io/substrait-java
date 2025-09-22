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

import io.substrait.spark.expression.{ToAggregateFunction, ToWindowFunction}

import io.substrait.extension.SimpleExtension

import java.util.Collections

import scala.collection.JavaConverters
import scala.collection.JavaConverters.asScalaBufferConverter

object SparkExtension {
  final val file = "/spark.yml"

  private val SparkImpls: SimpleExtension.ExtensionCollection =
    SimpleExtension.load(getClass.getResourceAsStream(file))

  private val EXTENSION_COLLECTION: SimpleExtension.ExtensionCollection =
    SimpleExtension.loadDefaults()

  val COLLECTION: SimpleExtension.ExtensionCollection = EXTENSION_COLLECTION.merge(SparkImpls)

  lazy val SparkScalarFunctions: Seq[SimpleExtension.ScalarFunctionVariant] = {
    val ret = new collection.mutable.ArrayBuffer[SimpleExtension.ScalarFunctionVariant]()
    ret.appendAll(EXTENSION_COLLECTION.scalarFunctions().asScala)
    ret.appendAll(SparkImpls.scalarFunctions().asScala)
    ret
  }

  val toAggregateFunction: ToAggregateFunction = ToAggregateFunction(
    JavaConverters.asScalaBuffer(EXTENSION_COLLECTION.aggregateFunctions()))

  val toWindowFunction: ToWindowFunction = ToWindowFunction(
    JavaConverters.asScalaBuffer(EXTENSION_COLLECTION.windowFunctions())
  )
}
