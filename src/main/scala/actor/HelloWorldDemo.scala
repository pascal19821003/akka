package actor

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}

class HelloActor extends Actor {
  def receive: Receive = {
    case "hi" =>
      println("hello")
      context.stop(self)
  }
}

class Terminator(ref: ActorRef) extends Actor {
  context watch ref

  def receive = {
    case Terminated(_) => context.system.terminate()
  }
}

object HelloWorldDemo extends App {
  val system = ActorSystem()
  val a = system.actorOf(Props[HelloActor])
  a ! "hi"
  system.actorOf(Props(classOf[Terminator], a))
}
