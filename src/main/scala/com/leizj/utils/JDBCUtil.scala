package com.leizj.utils

import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

object JDBCUtil {
  //初始化连接池
  lazy val connection: Connection = init().getConnection

  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://localhost:3306/spark_streaming?useUnicode=true&characterEncoding=UTF-8")
    properties.setProperty("username", "root")
    properties.setProperty("password", "1234")
    properties.setProperty("maxActive", "50")
    DruidDataSourceFactory.createDataSource(properties)
  }

}
