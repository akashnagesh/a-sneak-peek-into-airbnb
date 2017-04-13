package actors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import kafkaClients.{KafkaDeSerializers, KafkaRecommendationRequestProducer}
import org.apache.commons.lang3.StringUtils

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

      println("inside actor defaiult receive")

      kafkaProducer.publishMessage("key1", msg)
      kafkaClientManagerActor ! KafkaConsumerClientManagerActor.GetRecommendation("key1")
      // out ! msg + "appending this from server"
      context.become(onConsumerMessageBehavior)
    }
  }

  def onConsumerMessageBehavior: Receive = {
    case msg: String =>

      println("inside changed behavior")
      val consumerRecord = kafkaConsumer.consumeMessage("key1", StringUtils.EMPTY, KafkaDeSerializers.STRING_DESERIALIZER,
        KafkaDeSerializers.STRING_DESERIALIZER, "topic1")

      println(consumerRecord + "==============")
      for (record <- consumerRecord) yield {
        println("consumer recordd " + record.value())
        out ! (record.value() + "after round trip  from kafka")
      }
      self ! PoisonPill
  }
}
