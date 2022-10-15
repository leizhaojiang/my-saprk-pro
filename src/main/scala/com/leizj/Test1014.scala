package com.leizj

import scala.collection.mutable

object Test1014 {

  private val dbSet = new mutable.HashSet[String]

  def main(args: Array[String]): Unit = {
    dbSet += "123123123"
    println(dbSet.size)
  }

}
