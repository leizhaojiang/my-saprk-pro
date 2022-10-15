package com.leizj

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_test01 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-01").set("spark.default.parallelism", "10")
    val sc = new SparkContext(sparkConf)

    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    sparkSession.readStream
      .load()
    distData.foreach(value => {
      println(s"$value, thread-id : ${Thread.currentThread().getId}")
    })
  }

}
