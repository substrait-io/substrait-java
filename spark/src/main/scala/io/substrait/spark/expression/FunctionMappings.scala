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
package io.substrait.spark.expression

import io.substrait.spark.utils.Util

import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType}

import scala.reflect.ClassTag

trait Sig {
  def name: String
  def expClass: Class[_]
  def makeCall(args: Seq[Expression]): Expression
}

case class GenericSig(expClass: Class[_], name: String, builder: Seq[Expression] => Expression)
  extends Sig {
  override def makeCall(args: Seq[Expression]): Expression = {
    builder(args)
  }
}

case class SpecialSig(
    expClass: Class[_],
    name: String,
    key: Option[String],
    builder: Seq[Expression] => Expression)
  extends Sig {
  override def makeCall(args: Seq[Expression]): Expression = {
    builder(args)
  }
}

object DateFunction {
  def unapply(e: Expression): Option[Seq[Expression]] = e match {
    case DateAdd(startDate, days) => Some(Seq(startDate, days))
    // The following map to the Substrait `extract` function.
    case Year(date) => Some(Seq(Enum("YEAR"), date))
    case Quarter(date) => Some(Seq(Enum("QUARTER"), Enum("ONE"), date))
    case Month(date) => Some(Seq(Enum("MONTH"), Enum("ONE"), date))
    case DayOfMonth(date) => Some(Seq(Enum("DAY"), Enum("ONE"), date))
    case _ => None
  }

  def unapply(name_args: (String, Seq[Expression])): Option[Expression] = name_args match {
    case ("add:date_i32", Seq(startDate, days)) => Some(DateAdd(startDate, days))
    case ("extract", Seq(Enum("YEAR"), date)) => Some(Year(date))
    case ("extract", Seq(Enum("QUARTER"), Enum("ONE"), date)) => Some(Quarter(date))
    case ("extract", Seq(Enum("MONTH"), Enum("ONE"), date)) => Some(Month(date))
    case ("extract", Seq(Enum("DAY"), Enum("ONE"), date)) => Some(DayOfMonth(date))
    case _ => None
  }
}

object TrimFunction {
  def unapply(e: Expression): Option[Seq[Expression]] = e match {
    case StringTrim(srcStr, trimStr) => Some(Seq(srcStr) ++ trimStr)
    case StringTrimLeft(srcStr, trimStr) => Some(Seq(srcStr) ++ trimStr)
    case StringTrimRight(srcStr, trimStr) => Some(Seq(srcStr) ++ trimStr)
    case _ => None
  }

  def unapply(name_args: (String, Seq[Expression])): Option[Expression] = name_args match {
    case ("trim", Seq(srcStr)) => Some(StringTrim(srcStr))
    case ("trim", Seq(srcStr, trimStr)) => Some(StringTrim(srcStr, trimStr))
    case ("ltrim", Seq(srcStr)) => Some(StringTrimLeft(srcStr))
    case ("ltrim", Seq(srcStr, trimStr)) => Some(StringTrimLeft(srcStr, trimStr))
    case ("rtrim", Seq(srcStr)) => Some(StringTrimRight(srcStr))
    case ("rtrim", Seq(srcStr, trimStr)) => Some(StringTrimRight(srcStr, trimStr))
    case _ => None
  }
}

class FunctionMappings {

  private def s[T <: Expression: ClassTag](name: String): GenericSig = {
    val builder = FunctionRegistryBase.build[T](name, None)._2
    GenericSig(scala.reflect.classTag[T].runtimeClass, name, builder)
  }

  private def ss[T <: Expression: ClassTag](signature: String): SpecialSig = {
    val (name, key) = if (signature.contains(":")) {
      (signature.split(':').head, Some(signature))
    } else {
      (signature, None)
    }
    val builder = (args: Seq[Expression]) =>
      (signature, args) match {
        case DateFunction(expr) => expr
        case TrimFunction(expr) => expr
        case _ =>
          throw new UnsupportedOperationException(s"Cannot convert $signature with arguments $args")
      }
    SpecialSig(scala.reflect.classTag[T].runtimeClass, name, key, builder)
  }

  val SCALAR_SIGS: Seq[Sig] = Seq(
    s[Add]("add"),
    s[Subtract]("subtract"),
    s[Multiply]("multiply"),
    s[Divide]("divide"),
    s[Abs]("abs"),
    s[Remainder]("modulus"),
    s[Round]("round"),
    s[Floor]("floor"),
    s[Ceil]("ceil"),
    s[Pow]("power"),
    s[Exp]("exp"),
    s[Sqrt]("sqrt"),
    s[Sin]("sin"),
    s[Cos]("cos"),
    s[Tan]("tan"),
    s[Asin]("asin"),
    s[Acos]("acos"),
    s[Atan]("atan"),
    s[Atan2]("atan2"),
    s[Sinh]("sinh"),
    s[Cosh]("cosh"),
    s[Tanh]("tanh"),
    s[Asinh]("asinh"),
    s[Acosh]("acosh"),
    s[Atanh]("atanh"),
    s[Log]("ln"),
    s[Log10]("log10"),
    s[And]("and"),
    s[Or]("or"),
    s[Not]("not"),
    s[LessThan]("lt"),
    s[LessThanOrEqual]("lte"),
    s[GreaterThan]("gt"),
    s[GreaterThanOrEqual]("gte"),
    s[EqualTo]("equal"),
    s[EqualNullSafe]("is_not_distinct_from"),
    s[IsNull]("is_null"),
    s[IsNotNull]("is_not_null"),
    s[EndsWith]("ends_with"),
    s[Like]("like"),
    s[Contains]("contains"),
    s[StartsWith]("starts_with"),
    s[Substring]("substring"),
    s[Upper]("upper"),
    s[Lower]("lower"),
    s[Concat]("concat"),
    s[Coalesce]("coalesce"),
    s[ShiftLeft]("shift_left"),
    s[ShiftRight]("shift_right"),
    s[ShiftRightUnsigned]("shift_right_unsigned"),
    s[BitwiseAnd]("bitwise_and"),
    s[BitwiseOr]("bitwise_or"),
    s[BitwiseXor]("bitwise_xor"),

    // trim functions require special handling
    ss[StringTrim]("trim"),
    ss[StringTrimLeft]("ltrim"),
    ss[StringTrimRight]("rtrim"),

    // date/time functions require special handling
    ss[DateAdd]("add:date_i32"),
    ss[Year]("extract"),
    ss[Quarter]("extract"),
    ss[Month]("extract"),
    ss[DayOfMonth]("extract")
  )

  val AGGREGATE_SIGS: Seq[Sig] = Seq(
    s[Sum]("sum"),
    s[Average]("avg"),
    s[Count]("count"),
    s[Min]("min"),
    s[Max]("max"),
    s[First]("any_value"),
    s[HyperLogLogPlusPlus]("approx_count_distinct"),
    s[StddevSamp]("std_dev")
  )

  val WINDOW_SIGS: Seq[Sig] = Seq(
    s[RowNumber]("row_number"),
    s[Rank]("rank"),
    s[DenseRank]("dense_rank"),
    s[PercentRank]("percent_rank"),
    s[CumeDist]("cume_dist"),
    s[NTile]("ntile"),
    s[Lead]("lead"),
    s[Lag]("lag"),
    s[NthValue]("nth_value")
  )
}

object FunctionMappings extends FunctionMappings
