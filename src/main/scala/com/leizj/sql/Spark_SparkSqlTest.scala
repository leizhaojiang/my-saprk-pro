package com.leizj.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Spark_SparkSqlTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val df: DataFrame = sparkSession.read.json("datas/user.json")

    df.createOrReplaceTempView("user")
    sparkSession.sql("select * from user").show

    df.select($"age" + 1).show

    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark_streaming")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()

    sparkSession.stop()
  }

}
