package actors

import actors.ConsumerActor.ReadDataFromKafka
import actors.KafkaConsumerClientManagerActor.{GetRecommendation, RecommededListing}
import akka.actor.{Actor, ActorRef}
import kafkaClients.KafkaRecommendationResponseConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.mutable

/**
  * Created by akashnagesh on 4/13/17.
  */

object KafkaConsumerClientManagerActor {

  case class GetRecommendation(userId: String)

  case class RecommededListing(userId: String, value: String)

}

class KafkaConsumerClientManagerActor(consumer: ActorRef) extends Actor {

  var bufferMap = new mutable.HashMap[String, ActorRef]()

  override def receive = {
    case GetRecommendation(userId) => {
      bufferMap.put(userId, sender())
      consumer ! ReadDataFromKafka
    }
    case RecommededListing(userId, value) => {
      bufferMap.get(userId).foreach(x => x ! value)
    }
  }
}


object ConsumerActor {

  case class ReadDataFromKafka()

}

class ConsumerActor(consumerClient: KafkaRecommendationResponseConsumer) extends Actor {
  var foundRecord = false
  var valueFromKafka: ConsumerRecords[String, String] = null

  override def receive = {
    case ReadDataFromKafka() => {
      while (!foundRecord) {
        valueFromKafka = consumerClient.consumeMessage()
        if (!valueFromKafka.isEmpty) {
          foundRecord = true
        }
      }
      val v = valueFromKafka.iterator().next()
      sender() ! RecommededListing(v.key(), v.value())
    }
  }
}


