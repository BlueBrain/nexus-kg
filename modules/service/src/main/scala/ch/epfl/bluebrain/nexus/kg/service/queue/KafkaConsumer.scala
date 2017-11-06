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

  final def start[T](consumerSettings: ConsumerSettings[String, String],
                     index: T => Future[Unit],
                     topic: String)(implicit as: ActorSystem, ec: ExecutionContext, D: Decoder[T]): Future[Done] = {
    implicit val mt: ActorMaterializer = ActorMaterializer()

    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(topic))
      .mapAsync(1) { msg =>
        decode[T](msg.record.value) match {
          case Right(event) =>
            log.debug(s"Received message: $msg")
            index(event).map(_ => msg.committableOffset)
          case Left(e) => Future.failed(e)
        }
      }
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
        batch.updated(elem)
      }
      .mapAsync(1)(_.commitScaladsl())
      .runWith(Sink.ignore)
  }

}
