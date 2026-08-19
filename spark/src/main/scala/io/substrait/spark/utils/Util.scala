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
package io.substrait.spark.utils

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object Util {

  val SECONDS_PER_DAY: Long = 24 * 60 * 60
  val MICROS_PER_SECOND: Long = 1000 * 1000
  val MICROSECOND_PRECISION = 6 // for PrecisionTimestamp(TZ) and IntervalDay types

  /** Indexed by exponent, which [[toMicroseconds]] bounds to 0..MICROSECOND_PRECISION. */
  private val POWERS_OF_TEN: Array[Long] =
    Array(1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L)

  /**
   * Checks that a Substrait fractional-second precision is one a Spark type can carry.
   *
   * Spark has a single microsecond-based representation for each of these types, so the precision
   * has to be exactly microseconds. A coarser one describes values in different units, and a type
   * conversion has no values in hand to rescale — see [[toMicroseconds]], which is what the literal
   * conversions use where a value is available.
   */
  def assertMicroseconds(precision: Int): Unit = {
    if (precision != MICROSECOND_PRECISION) {
      throw new UnsupportedOperationException(
        s"Unsupported precision: $precision. Spark stores time values as microseconds, so a type " +
          s"must declare a precision of exactly $MICROSECOND_PRECISION; a value at a coarser " +
          s"precision is converted by rescaling it")
    }
  }

  /**
   * Rescales a sub-second value from the given Substrait precision to the microseconds Spark uses
   * as its physical representation.
   *
   * A precision coarser than microseconds carries fewer digits, not different ones, so the value is
   * exact after scaling. A finer one does not fit and is rejected rather than rounded.
   */
  def toMicroseconds(value: Long, precision: Int): Long = {
    if (precision < 0 || precision > MICROSECOND_PRECISION) {
      throw new UnsupportedOperationException(
        s"Unsupported precision: $precision. Spark stores time values as microseconds, " +
          s"so the precision must be between 0 and $MICROSECOND_PRECISION")
    }
    Math.multiplyExact(value, POWERS_OF_TEN(MICROSECOND_PRECISION - precision))
  }

  /**
   * Compute the cartesian product for n lists.
   *
   * <p>Based on <a
   * href="https://thomas.preissler.me/blog/2020/12/29/permutations-using-java-streams">Soln by
   * Thomas Preissler</a></a>
   */
  def crossProduct[T](lists: Seq[Seq[T]]): Seq[Seq[T]] = {
    if (lists.isEmpty) return lists

    /** list [a, b], element 1 =>  list + element => [a, b, 1] */
    val appendElementToList: (Seq[T], T) => Seq[T] =
      (list, element) => list :+ element

    /** ([a, b], [1, 2]) ==> [a, b, 1], [a, b, 2] */
    val appendAndGen: (Seq[T], Seq[T]) => Seq[Seq[T]] =
      (list, elemsToAppend) => elemsToAppend.map(e => appendElementToList(list, e))

    val firstListToJoin = lists.head
    val startProduct = appendAndGen(new ArrayBuffer[T].toSeq, firstListToJoin)

    /** ([ [a, b], [c, d] ], [1, 2]) -> [a, b, 1], [a, b, 2], [c, d, 1], [c, d, 2] */
    val appendAndGenLists: (Seq[Seq[T]], Seq[T]) => Seq[Seq[T]] =
      (products, toJoin) => products.flatMap(product => appendAndGen(product, toJoin))
    lists.tail.foldLeft(startProduct)(appendAndGenLists)
  }

  def seqToOption[T](s: Seq[Option[T]]): Option[Seq[T]] = {
    @tailrec
    def seqToOptionHelper(s: Seq[Option[T]], accum: Seq[T] = Seq[T]()): Option[Seq[T]] = {
      s match {
        case Seq(Some(head)) =>
          Option(accum :+ head)
        case Seq(Some(head), tail @ _*) =>
          seqToOptionHelper(tail, accum :+ head)
        case _ => None
      }
    }
    seqToOptionHelper(s)
  }

}
