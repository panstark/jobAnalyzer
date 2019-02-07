package com.sea.spark

import akka.actor.{Actor, ActorSystem, Props}

object AkkaDemo {

  class Actor1 extends Actor{
    override def receive: Receive = {
      case msg:String=>{
        println("actor1 print"+msg)
      }
    }
  }

  class Actor2 extends Actor{
    override def receive: Receive = {
      case msg:String=>{
        println("actor2 print"+msg)
      }
    }
  }


  def main(args: Array[String]): Unit = {
   /* val sys = ActorSystem("mySys")
    val act1 = sys.actorOf(Props[Actor1])
    act1.!("I am actor1")*/

    val name = "12345aa你好哦"
    val nameStr = name.replaceAll("[\\u4e00-\\u9fa5]","")
    println(nameStr)
  }

}
