package actor

import actor.AkkaAskDemo.res
import actor.HelloWorldDemo.{a, system}
import akka.actor._
import akka.pattern.ask

import scala.concurrent.Await
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class HelloActor extends Actor {
  def receive: Receive = {
    case "hi" =>
      println("hello")
      context.stop(self)
    case "hi-ask" =>
      sender ! "hello"
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

object AkkaAskDemo extends App {
  val system = ActorSystem()
  val a = system.actorOf(Props[HelloActor], "hello")
  system.actorOf(Props(classOf[Terminator], a))
  implicit val timeout = Timeout(5 seconds)
  val res = a ? "hi-ask"
  val x = Await.result(res, 2 seconds)
  println(x)
  system.stop(a)
}

object ActorPathDemo extends App {
  val system = ActorSystem()
  val a = system.actorOf(Props[HelloActor])
  system.actorOf(Props(classOf[Terminator], a))

  implicit val timeout = Timeout(5 seconds)
  val f = (system.actorSelection(a.path) ? Identify(None))

  f.onSuccess {
    case ActorIdentity(_, Some(ref)) =>
      ref ! "hi"
    case _ => println("cant get actor reference")
  }

  system.stop(a)
}
