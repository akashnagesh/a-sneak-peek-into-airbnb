package actors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import kafkaClients.KafkaRecommendationRequestProducer

/**
  * Created by akashnagesh on 4/11/17.
  */
object RecommendationWebSocketActor {
  def props(out: ActorRef, kafkaProducer: KafkaRecommendationRequestProducer,
            kafkaClientManagerActor: ActorRef) =
    Props(new RecommendationWebSocketActor(out, kafkaProducer, kafkaClientManagerActor))
}

class RecommendationWebSocketActor(val out: ActorRef, val kafkaProducer: KafkaRecommendationRequestProducer,
                                   val kafkaClientManagerActor: ActorRef) extends Actor {

  def receive = {
    case msg: String => {

      println("inside actor default receive")

      kafkaProducer.publishMessage(msg, msg)
      kafkaClientManagerActor ! KafkaConsumerClientManagerActor.GetRecommendation(msg)
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
