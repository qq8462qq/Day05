package cn.itcast.demo

import akka.actor.Actor

object SenderActor extends Actor{
  override def receive: Receive = {
    case "start" =>{
      println("SenderActor:接收到start消息")
      val receiverActor = context.actorSelection("akka://actorSystem/user/receiverActor")
      receiverActor ! SubmitTaskMessage("提交任务!")

    }
    case SuccessSubmitTaskMessage(message)=>
      println(s"SenderActor:接收到ReciverActor执行任务成功消息! ${message}!")
  }
}
