package com.leizj.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Spark_SparkSql_Mysql {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-mysql")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //读取数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/kuafu_db")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
//      .option("dbtable", "person")
      .option("dbtable", "(select * from person limit 10) as p")
      .load()
    df.show

    //保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/kuafu_db")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
      .option("dbtable", "person_1")
      .mode(SaveMode.Append)
      .save()

    spark.stop()

  }

}
