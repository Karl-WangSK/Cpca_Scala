package com

import java.util

import scala.collection.mutable

object Test {
  def main(args: Array[String]): Unit = {
    val strings = Array( "上海市浦东新区","浙江省温州市瓯海区" )
    val cpca = new Cpca()
    val list: mutable.Seq[String] = cpca.transform_arr(strings)
    for (elem <-list)  {
      println(elem)
    }

    import scala.collection.JavaConversions._
    val addrList: util.ArrayList[String] = Cpca.get_dic()
    for (elem <- addrList.toList){
      println(elem)
    }

  }

}
