package cn.itcast.demo02

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object SchedulerDemo {
  //1创建一个Actor,用来接收消息,打印消息
  object ReceivActor extends Actor{
    override def receive: Receive = {
      case x=>println(x)
    }
  }
  //2构建ActorSystem,加载Actore
  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem("actorSystem",ConfigFactory.load())
    val receiveActor = actorSystem.actorOf(Props(ReceivActor))
    //3启动Scheduler,定期发送消息给Actor
    //导入一个隐式转换
    import scala.concurrent.duration._
    //导入隐式参数
    import actorSystem.dispatcher
    actorSystem.scheduler.schedule(0 seconds,
      1 seconds,
      receiveActor,"王八蛋!")
  }


}
