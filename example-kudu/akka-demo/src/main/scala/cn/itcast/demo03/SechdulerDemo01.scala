package cn.itcast.demo03

import akka.actor.{Actor, ActorSystem, Props}
import cn.itcast.demo03.SechdulerDemo01.SechdulerActor
import com.typesafe.config.ConfigFactory

object SechdulerDemo01 {
  object SechdulerActor extends Actor {
    override def receive: Receive = {
      case "timer" => println("收到消息...")
    }
  }


}
object AkkaSchedulerDemo {
  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem("SimpleAkkaDemo", ConfigFactory.load())

    val actorRef = actorSystem.actorOf(Props(SechdulerActor), "sechdulerActor")

    import actorSystem.dispatcher
    import scala.concurrent.duration._

    actorSystem.scheduler.schedule(0 seconds, 1 seconds) {
      actorRef ! "timer"
    }
  }
}