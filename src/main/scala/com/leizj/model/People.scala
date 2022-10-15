package com.leizj.model

import scala.beans.BeanProperty

class People extends Serializable {

  @BeanProperty
  var name: String = _
  @BeanProperty
  var age: Int = _

  override def toString: String = {
    s"""{\"name\": \"$name\", \"age\": $age}"""
  }



  def this(name: String, age: Int) {
    this()
    this.name = name
    this.age = age
  }
}