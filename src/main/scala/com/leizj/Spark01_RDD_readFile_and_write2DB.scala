package com.leizj

import com.alibaba.fastjson2.JSON
import com.leizj.model.People
import com.leizj.utils.JDBCUtil
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.util.Random

object Spark01_RDD_readFile_and_write2DB {

  val lines: Int = 1000000

  val sc: SparkContext = getSparkContext()

  val ss: SparkSession = getSparkSession()

  def getSparkContext(): SparkContext = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
      .set("spark.default.parallelism", "12")
    new SparkContext(sparkConf)
  }

  def getSparkSession(): SparkSession = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
      .set("spark.default.parallelism", "12")
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  def test_RDD_read_and_writeToDb(): Unit = {

    val df = ss.read.json("output/part-00000")
    df.show(20)

    val conn = JDBCUtil.connection
    val pstm = conn.prepareStatement(
      """create table if not exists people(
        |name varchar(10) null,
        |age int(10) null
        |)""".stripMargin)
    pstm.executeUpdate()
    conn.close()
    pstm.close()

    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark_streaming")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
      .option("dbtable", "people")
      .mode(SaveMode.Append)
      .save()
    sc.stop()

  }

  def test_RDD_write(): Unit = {
    val list = List("雷", "付", "张", "樊", "信", "刘", "李", "赵", "田", "白")
    val rdd = sc.makeRDD(list)

    rdd.saveAsTextFile("output")
    sc.stop()
  }

  def test_RDD_write_people(): Unit = {
    deleteDir(new File("output"))
    val lastnames = List("雷", "付", "张", "樊", "信", "刘", "李", "赵", "田", "白")
    val firstname_1 = List("化", "江", "国", "娟", "蕾", "花", "婷", "平", "和", "海")
    val firstname_2 = List("化", "江", "国", "娟", "蕾", "花", "婷", "平", "和", "海")
    val peoples: ListBuffer[People] = ListBuffer()
    var count: Int = 0
    var i: Int = 1
    while (count < lines) {
      val fullName = lastnames(Random.nextInt(10)) + firstname_1(Random.nextInt(10)) + firstname_2(Random.nextInt(10))
      val people = new People(fullName, Random.nextInt(60))
      peoples.append(people)

      if (peoples.length >= 100000) {
        val rdd = sc.makeRDD(peoples).map(_.toString)
        rdd.saveAsTextFile("output/test-" + i)
        i += 1
        peoples.clear()
        count += 100000
        println(s"已持久化条数：" + count/10000 + "w")
      }
    }

  }

  def deleteDir(file: File): Unit = {
    if (!file.exists()) return
    file.listFiles().foreach(f => {
      if (f.isDirectory) {
        deleteDir(file)
      } else {
        f.delete()
      }
    })
    file.delete()
  }


  def readFile2Db(path: String): Unit = {

    val rdd = sc.textFile(path).map(s => {
      val json = JSON.parseObject(s)
      val name = json.getString("name")
      val age = json.getIntValue("age")
      Row(name, age)
    })

    val schema = StructType(List(
      StructField("name", DataTypes.StringType, nullable = true),
      StructField("age", DataTypes.IntegerType, nullable = true)
    ))

    ss.createDataFrame(rdd,schema)
      .write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark_test")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
      .option("dbTable", "people_test_2")
      .mode(SaveMode.Append)
      .save()

  }

  def main(args: Array[String]): Unit = {

//    test_RDD_write_people()
    val start = System.currentTimeMillis()
    readFile2Db("output/test-*")

    println((System.currentTimeMillis() - start) / 1000)
  }


}
