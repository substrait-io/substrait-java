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

package io.substrait.spark.expression

import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Unevaluable}
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * For internal use only. This represents the equivalent of a Substrait enum parameter type for use
 * during conversion. It must not become part of a final Spark logical plan.
 *
 * @param value
 *   The enum string value.
 */
case class Enum(value: String) extends LeafExpression with Unevaluable {
  override def nullable: Boolean = false

  override def dataType: DataType = NullType

  override def equals(that: Any): Boolean = that match {
    case Enum(other) => other == value
    case _ => false
  }
}
