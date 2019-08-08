package cn.itcast.demo

import akka.actor.Actor

object ReceiverActor extends Actor{
  override def receive: Receive = {
    case SubmitTaskMessage(message) =>{
      println(s"ReceiverActor:接收到消息!${message}")

      sender ! SuccessSubmitTaskMessage(s"成功执行任务消息!")
    }
  }
}
