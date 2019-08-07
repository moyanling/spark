package org.mo39.fmbh.spark.examples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, DecimalType}

trait SparkTest {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master(master = "local")
    .appName(name = "Test App")
    .getOrCreate()

  lazy val df: DataFrame = {
    import spark.implicits._
    val df = Seq(
      (
        "fmbh",
        "fmbh",
        "true",
        "2019-02-15",
        "2019-02-15 16:39:39",
        "interval   -5  years  23   month",
        "0.00000001",
        "0.0001",
        "1",
        "123",
        "12345",
        "12",
        "3.1415926",
        null
      )).toDF(colNames = "String",
              "Binary",
              "Boolean",
              "Date",
              "Timestamp",
              "CalendarInterval",
              "Double",
              "Float",
              "Byte",
              "Integer",
              "Long",
              "Short",
              "Decimal",
              "Null")
    val resultDF = df.select(
      df("String").cast(DataTypes.StringType),
      df("Binary").cast(DataTypes.BinaryType),
      df("Boolean").cast(DataTypes.BooleanType),
      df("Date").cast(DataTypes.DateType),
      df("Timestamp").cast(DataTypes.TimestampType),
      df("CalendarInterval").cast(DataTypes.CalendarIntervalType),
      df("Double").cast(DataTypes.DoubleType),
      df("Float").cast(DataTypes.FloatType),
      df("Byte").cast(DataTypes.ByteType),
      df("Integer").cast(DataTypes.IntegerType),
      df("Long").cast(DataTypes.LongType),
      df("Short").cast(DataTypes.ShortType),
      df("Decimal").cast(DecimalType(10, 0)),
      df("Null").cast(DataTypes.NullType)
    )

    /*
     * root
     * |-- String: string (nullable = true)
     * |-- Binary: binary (nullable = true)
     * |-- Boolean: boolean (nullable = true)
     * |-- Date: date (nullable = true)
     * |-- Timestamp: timestamp (nullable = true)
     * |-- CalendarInterval: calendarinterval (nullable = true)
     * |-- Double: double (nullable = true)
     * |-- Float: float (nullable = true)
     * |-- Byte: byte (nullable = true)
     * |-- Integer: integer (nullable = true)
     * |-- Long: long (nullable = true)
     * |-- Short: short (nullable = true)
     * |-- Decimal: decimal(10,0) (nullable = true)
     * |-- Null: null (nullable = true)
     */
    // resultDF.printSchema()

    /*
     * +------+-------------+-------+----------+-------------------+--------------------+------+------+----+-------+-----+-----+-------+----+
     * |String|       Binary|Boolean|      Date|          Timestamp|    CalendarInterval|Double| Float|Byte|Integer| Long|Short|Decimal|Null|
     * +------+-------------+-------+----------+-------------------+--------------------+------+------+----+-------+-----+-----+-------+----+
     * |  fmbh|[66 6D 62 68]|   true|2019-02-15|2019-02-15 16:39:39|interval -3 years...|1.0E-8|1.0E-4|   1|    123|12345|   12|      3|null|
     * +------+-------------+-------+----------+-------------------+--------------------+------+------+----+-------+-----+-----+-------+----+
     */
    // resultDF.show()
    resultDF
  }

}
