package controllers

import javax.inject.{Inject, _}

import dataAccessLayer.{UserActionMessages, UserDalImpl}
import models.{FormsData, User}
import org.apache.commons.lang3.StringUtils
import play.api.mvc._
import play.api.i18n.{I18nSupport, MessagesApi}

import scala.concurrent.{ExecutionContext, Future}

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class HomeController @Inject()(val messagesApi: MessagesApi)(userDalImpl: UserDalImpl)(implicit ec: ExecutionContext) extends Controller with I18nSupport {

  /**
    * Create an Action to render an HTML page with a welcome message.
    * The configuration in the `routes` file means that this method
    * will be called when the application receives a `GET` request with
    * a path of `/`.
    */


  def index = Action {
    Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)(StringUtils.EMPTY))
  }

  def userLogin = Action { implicit request =>
    FormsData.userForm.bindFromRequest().fold(
      formWithErrors => BadRequest,
      userTuple => Ok(s"User ${userTuple._1} logged in successfully")
    )
  }

  def createUser = Action.async { implicit request =>
    FormsData.createUserForm.bindFromRequest().fold(
      errorForm => Future.successful(Ok),
      user => {
        val user1 = User(0, user.name, user.age, user.email, user.password)
        userDalImpl.addUser(user1)
          .map(someMes => someMes match {
            case UserActionMessages.emailAlreadyExists => Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)("Email Id Already Exists"))
            case UserActionMessages.genericError => Ok("Unable to create an Account.Try After Sometime")
            case _ => Ok("New Page will come here ")
          })
      }
    )

  }
}
