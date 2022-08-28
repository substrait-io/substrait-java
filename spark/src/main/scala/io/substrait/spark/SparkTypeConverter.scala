package io.substrait.spark

import io.substrait.`type`.{NamedStruct, Type}
import org.apache.spark.sql.types._
import org.json4s.scalap.scalasig.ClassFileParser.field

object SparkTypeConverter {

  def toNamedStruct(schema: StructType): io.substrait.`type`.NamedStruct = {
    val creator = Type.withNullability(true)
    val names = new java.util.ArrayList[String]
    val children = new java.util.ArrayList[Type]
    schema.fields.foreach(field => {
      names.add(field.name)
      children.add(convert(field.dataType, field.nullable))
    })
    val struct = creator.struct(children)
    NamedStruct.of(names, struct)
  }

  def convert(dataType: DataType, nullable: Boolean): io.substrait.`type`.Type = {
    val creator = Type.withNullability(nullable)
    //spark sql data types: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    dataType match {
      case ByteType => creator.I8
      case ShortType => creator.I16
      case IntegerType => creator.I32
      case LongType => creator.I64
      case FloatType => creator.FP32
      case DoubleType => creator.FP64
      case decimalType: DecimalType =>
        if (decimalType.precision > 38) {
          throw new UnsupportedOperationException("unsupported decimal precision " + decimalType.precision)
        }
        creator.decimal(decimalType.precision, decimalType.scale);
      case StringType => creator.STRING
      case BinaryType => creator.BINARY
      case BooleanType => creator.BOOLEAN
      case TimestampType => creator.TIMESTAMP
      case DateType => creator.DATE
      case YearMonthIntervalType.DEFAULT => creator.INTERVAL_YEAR
      case DayTimeIntervalType.DEFAULT => creator.INTERVAL_DAY
      case ArrayType(elementType, containsNull) =>
        creator.list(convert(elementType, containsNull))
      case MapType(keyType, valueType, valueContainsNull) =>
        creator.map(convert(keyType, nullable = false), convert(valueType, valueContainsNull))
      case StructType(fields) =>
        // TODO: now we miss the nested StructType's field names,do we need them?
        //val names = new java.util.ArrayList[String]
        val children = new java.util.ArrayList[Type]
        fields.foreach(field => {
          //names.add(field.name)
          children.add(convert(field.dataType, field.nullable))
        })
        val struct = creator.struct(children)
        struct
      case _ =>
        throw new UnsupportedOperationException(String.format("Unable to convert the type " + field.toString))
    }
  }

}
