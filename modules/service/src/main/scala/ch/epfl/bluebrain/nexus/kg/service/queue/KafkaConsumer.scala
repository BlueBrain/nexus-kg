package ch.epfl.bluebrain.nexus.kg.service.queue

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import io.circe.Decoder
import io.circe.parser._
import journal.Logger

import scala.concurrent.{ExecutionContext, Future}

object KafkaConsumer {

  private val log = Logger[this.type]

  /**
    * Starts a Kafka consumer that reads messages from a particular ''topic'', decodes them into
    * events of type ''T'' and indexes them using the provided function.
    *
    * @param settings an instance of [[akka.kafka.ConsumerSettings]]
    * @param index    the indexing function that is applied to received events
    * @param topic    the Kafka topic to read messages from
    * @param as       an implicitly available actor system
    * @param D        an implicitly available Circe decoder to deserialize received messages
    * @tparam T       the event type
    */
  final def start[T](settings: ConsumerSettings[String, String], index: T => Future[Unit], topic: String)(
      implicit as: ActorSystem,
      D: Decoder[T]): Future[Done] = {
    implicit val ec: ExecutionContext  = as.dispatcher
    implicit val mt: ActorMaterializer = ActorMaterializer()

    Consumer
      .committableSource(settings, Subscriptions.topics(topic))
      .mapAsync(1) { msg =>
        decode[T](msg.record.value) match {
          case Right(event) =>
            log.debug(s"Received message: $msg")
            index(event).map(_ => msg.committableOffset)
          case Left(e) =>
            log.error(s"Failed to decode message: $msg", e)
            Future.failed(e)
        }
      }
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
        batch.updated(elem)
      }
      .mapAsync(1)(_.commitScaladsl())
      .runWith(Sink.ignore)
  }

}
