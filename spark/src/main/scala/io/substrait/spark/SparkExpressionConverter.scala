package io.substrait.spark

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object SparkExpressionConverter {

  def convert(expr: Expression,
              inputPlan: LogicalPlan,
              inputRecordType: io.substrait.`type`.Type.Struct): io.substrait.expression.Expression = expr match {
    case _ =>
      throw new UnsupportedOperationException(String.format("Unable to convert the expr to a substrait Expression: " + expr))
  }
}
