package com.sea.spark

import java.io.IOException

object Test2 {

  def main(args: Array[String]): Unit = {
    val exception=try{
      throw new IOException
      "正常代码"
    }catch {
      case t:IndexOutOfBoundsException =>"这里是IndexOutOfBoundsException"
      case t:IOException=>"这里是IOException"
      case t:RuntimeException =>"这里是RuntimException"
    }finally {
        "这里是finally"
    }
    println(exception)

    val food = "corn"
    val result = food match{
      case "watermelon" =>"西瓜"
      case "potato" =>"土豆"
      case "corn"=>"玉米"
      case _=>"万事万物"
    }
    println(result)

    val list = new Array[String](10)
    for(v<-list){
      println(v)
    }

  }

  def sum(num1:Int,num2:Int): Int ={
    num1+num2
  }

  (num1:Int,num2:Int)=>{
    num1+num2
  }

  (num1:Int,num2:Int)=>num1+num2



}
