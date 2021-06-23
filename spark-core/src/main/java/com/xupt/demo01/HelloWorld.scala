package com.xupt.demo01

/**
  * @author Wnlife 
  */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello world.")
    val list: List[Int] = List(1, 2, 3)
    val logic: (Int) => Int = _ * 2
    list.map(logic).foreach(println)

  }
}
