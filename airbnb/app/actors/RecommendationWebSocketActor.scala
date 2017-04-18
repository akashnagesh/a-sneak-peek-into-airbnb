package actors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import kafkaClients.KafkaRecommendationRequestProducer

/**
  * Created by akashnagesh on 4/11/17.
  */
object RecommendationWebSocketActor {
  def props(out: ActorRef, kafkaProducer: KafkaRecommendationRequestProducer,
            kafkaClientManagerActor: ActorRef, user: String) =
    Props(new RecommendationWebSocketActor(out, kafkaProducer, kafkaClientManagerActor, user))
}

class RecommendationWebSocketActor(val out: ActorRef, val kafkaProducer: KafkaRecommendationRequestProducer,
                                   val kafkaClientManagerActor: ActorRef, val user: String) extends Actor {

  def receive = {
    case msg: String => {

      println("inside actor default receive")

      kafkaProducer.publishMessage(user, msg)
      kafkaClientManagerActor ! KafkaConsumerClientManagerActor.GetRecommendation(user)
      // out ! msg + "appending this from server"
      context.become(onConsumerMessageBehavior)
    }
  }

  def onConsumerMessageBehavior: Receive = {
    case msg: String => {
      println("final msg in actor")
      out ! msg
      self ! PoisonPill
    }
  }
}
