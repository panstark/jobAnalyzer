package com.sea.spark

import sun.tools.jconsole.Plotter


object fucntionPractice {

  def main(args: Array[String]): Unit = {
    //1.柯里化
   def cookFood(potato:String,tomato:String,cook:(String)=>Unit): Unit ={
     val food = potato+tomato
     cook("做熟"+food)
   }
    def cookFood1(potato:String,tomato:String)(cook:(String)=>Unit): Unit ={
      val food = potato+tomato
      cook("做熟"+food)
    }

    def cookFood2(potato:String)(tomato:String)(cook:(String)=>Unit): Unit ={
      val food = potato+tomato
      cook(food)
    }

    //2、控制结构
    cookFood1("土豆","西红柿"){
      println(_)
    }

    val list = List(1,2,3,4,5,6)
    list.foreach((x:Int)=>{val r = x*10;println(r)})
    list.foreach((x:Int)=>println(x))

    val cook1 = (food1:String,food2:String)=>{println("我要做"+food1+","+food2)}

    val hamburger = cookHamburger("面包片","酱牛肉",cooKMethod)
    print(hamburger)

    val cook= getHowCook("hamburger");

    print(cook("面包","牛肉"))



  }

  def cookHamburger(food1:String,food2:String,cookMethod:(String,String)=>String):String={
    val hamburger = cookMethod(food1,food2)
    return hamburger
  }

  def cooKMethod(food1:String,food2:String): String ={
    "搅拌"+food1+"与"+food2+"，并进行烘烤"
  }

  def getHowCook(name:String):(String,String)=>String={
    if(name.equals("hamburger")){
      def cookHamburger(food1:String,food2:String): String ={
        "搅拌"+food1+"与"+food2+"，并进行烘烤"
      }
      return cookHamburger
    }else{
      def cookAnyThing(food1:String,food2:String):String={
        "弄熟"+food1+food2
      }
      return cookAnyThing;
    }
  }



}


class Person{
  def eat(food:String): Unit ={
    def cook(food:String): String ={
      "cook the "+ food
    }
    val meal = cook(food)
    print("people is eating"+meal);
  }

}