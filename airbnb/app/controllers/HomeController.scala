package controllers

import javax.inject.{Inject, _}

import actors.LoginActor
import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.util.Timeout
import dataAccessLayer.{UserActionMessages, UserDalImpl}
import models.{FormsData, User}
import org.apache.commons.lang3.StringUtils
import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class HomeController @Inject()(val messagesApi: MessagesApi)(userDalImpl: UserDalImpl)(system: ActorSystem)(implicit ec: ExecutionContext) extends Controller with I18nSupport {

  /**
    * Create an Action to render an HTML page with a welcome message.
    * The configuration in the `routes` file means that this method
    * will be called when the application receives a `GET` request with
    * a path of `/`.
    */

  val loginRouter = system.actorOf(Props(classOf[LoginActor], userDalImpl).withRouter(RoundRobinPool(10)), name = "LoginActor")


  def index = Action {
    Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)(StringUtils.EMPTY))
  }

  def userLogin = Action.async { implicit request =>
    implicit val timeout: Timeout = Timeout(2 seconds)
    FormsData.userForm.bindFromRequest().fold(
      formWithErrors => Future.successful(BadRequest),
      userTuple => {
        loginRouter ? LoginActor.GetUser(userTuple._1, userTuple._2)
      } map {
        case Some(user) => Ok(views.html.userMain("Welcome User"))
        case None => Ok("Invalid credentials")
      }
    )
  }

  def createUser = Action.async { implicit request =>
    implicit val timeout: Timeout = Timeout(2 seconds)
    FormsData.createUserForm.bindFromRequest().fold(
      errorForm => Future.successful(Ok),
      user => {
        val user1 = User(0, user.name, user.age, user.email, user.password)
        loginRouter ? LoginActor.CreateUser(user1)
      } map (someMes => someMes match {
        case UserActionMessages.emailAlreadyExists => Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)("Email Id Already Exists"))
        case UserActionMessages.genericError => Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)("Unable to create user. Please try again."))
        case _ => Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)("User account created! Login to use our service"))
      })
    )

  }
}
