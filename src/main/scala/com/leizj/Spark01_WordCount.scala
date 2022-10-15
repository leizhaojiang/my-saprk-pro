package com.leizj

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {
    //建立与sparkk框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //执行业务操作
    //1、读取文件，获取一行行的数据
    val lines: RDD[String] = sc.textFile("datas")

    val words = lines.flatMap(_.split(" "))

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    //2、将一行行的数据进行拆分，形成一个个的单词

    //3、对单词进行分组

    //4、对分组后的数据进行转换


  }
}
