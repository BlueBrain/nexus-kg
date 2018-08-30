package ch.epfl.bluebrain.nexus.kg.service.kafka

import java.util.concurrent.Future

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ProducerMessage._
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.persistence.query.Offset
import akka.stream.scaladsl.Flow
import ch.epfl.bluebrain.nexus.commons.service.persistence.SequentialTagIndexer
import ch.epfl.bluebrain.nexus.kg.service.kafka.key._
import io.circe.Encoder
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import shapeless.Typeable

/**
  * Helper class to send messages to different Kafka topics sharing only one instance
  * of a [[org.apache.kafka.clients.producer.KafkaProducer]]
  *
  * @param producer the shared [[KafkaProducer]] instance
  * @param topic    the kafka topic
  * @tparam Event   the generic event type
  */
// $COVERAGE-OFF$
class KafkaPublisher[Event: Encoder: Key](producer: KafkaProducer[String, String], topic: String) {

  /**
    * Manually sends a single message
    *
    * @param event the event to send
    * @return the metadata for the record acknowledge by the server
    */
  def send(event: Event): Future[RecordMetadata] = {
    val message = new ProducerRecord[String, String](topic, event.key, event.asJson.noSpaces)
    producer.send(message)
  }
}

object KafkaPublisher {

  private def flow[Event: Encoder: Key](producerSettings: ProducerSettings[String, String],
                                        topic: String): Flow[(Offset, String, Event), Offset, NotUsed] = {
    Flow[(Offset, String, Event)]
      .map {
        case (off, _, event) =>
          Message(new ProducerRecord[String, String](topic, event.key, event.asJson.noSpaces), off)
      }
      .via(Producer.flexiFlow(producerSettings))
      .map(_.passThrough)

  }

  /**
    * Starts publishing events to Kafka using a [[ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer]]
    *
    * @param projectionId projection to user for publishing events
    * @param pluginId query plugin ID
    * @param tag events with which tag to publish
    * @param name name of the actor
    * @param producerSettings Akka StreamsKafka producer settings
    * @param topic topic to publish to
    * @param as implicit ActorSystem
    * @tparam Event the generic event type
    * @return ActorRef for the started actor
    */
  final def startTagStream[Event: Encoder: Key: Typeable](projectionId: String,
                                                          pluginId: String,
                                                          tag: String,
                                                          name: String,
                                                          producerSettings: ProducerSettings[String, String],
                                                          topic: String)(implicit as: ActorSystem): ActorRef =
    SequentialTagIndexer.start(
      flow(producerSettings, topic),
      projectionId,
      pluginId,
      tag,
      name
    )
}
// $COVERAGE-ON$
