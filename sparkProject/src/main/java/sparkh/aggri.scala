package sparkh.aggri

import org.apache.spark.sql.SparkSession
import java.io.File
object aggri {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
    .getOrCreate()
    val sc=spark.sparkContext
    val sqlContext=spark.sqlContext
    //sqlContext.sql("show databases").show()
    //sqlContext.sql("use testdb").show()
   val df= spark.sql("select * from testdb.pro_part")
   
    df.show(false)
    /*
+----------+-------------------------+-------+------------+---------+
|product_id|purchase_date            |price  |country_name|date_part|
+----------+-------------------------+-------+------------+---------+
|p6        |2018-12-12T12:05:45+03:00|2548.75|China       |20181212 |
|p2        |2018-12-20T14:05:00+03:00|1985.75|India       |20181220 |
|p5        |2018-12-24T16:20:25+03:00|658.75 |China       |20181224 |
|p1        |2018-12-24T14:01:00+03:00|1951.75|India       |20181224 |
|p7        |2018-12-03T16:05:00+03:00|6589.75|India       |20181203 |
|p4        |2018-12-15T10:25:00+01:66|5000.0 |USA         |20181215 |
|p3        |2018-12-02T10:05:00+01:66|1985.75|USA         |20181202 |
+----------+-------------------------+-------+------------+---------+
     */
    
   //import org.apache.spark.sql.expressions.Window 
   // df.groupBy(windo, cols)
   import org.apache.spark.sql.functions._
    df.groupBy(col("purchase_date"),window(col("purchase_date"),"30 day")).agg(sum("price")).show()
    df.groupBy(col("purchase_date"),window(col("purchase_date"),"15 minute")).agg(sum("price")).show()
    df.groupBy(col("purchase_date"),window(col("purchase_date"),"1 week")).agg(sum("price")).show()
    df.groupBy(col("purchase_date"),window(col("purchase_date"),"1 day")).agg(sum("price")).show()
    
  }
}