package cn.itcast.demo

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
//开发
//开发
object Entrance3 {
  def main(args: Array[String]): Unit = {
    //实现一个Actor  trait
    //创建ActorSystem
    val actorSystem = ActorSystem("actorSystem",ConfigFactory.load())
    //加载Actor
    val senderActor = actorSystem.actorOf(Props(SenderActor),"senderActor")
    val receiverActor = actorSystem.actorOf(Props(ReceiverActor),"receiverActor")
    //main方法中发送一个start的消息
    senderActor ! "start"



  }
}
