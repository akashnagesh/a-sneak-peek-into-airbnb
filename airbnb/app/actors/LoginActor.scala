package actors

import actors.LoginActor.GetUser
import akka.actor.Actor
import models.User

/**
  * Created by akashnagesh on 4/9/17.
  */

object LoginActor {

  case class GetUser(email: String, password: String)

  case class CreateUser(user: User)

}

class LoginActor extends Actor {
  override def receive: Receive = {
    case GetUser(email: String, password: String) =>
    case GetUser(email: String, password: String) =>
  }
}
