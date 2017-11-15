package ch.epfl.bluebrain.nexus.kg.service.queue

import akka.Done
import akka.actor._
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.pattern.BackoffSupervisor.{CurrentChild, GetCurrentChild}
import akka.pattern.{Backoff, BackoffSupervisor, ask}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.google.common.annotations.VisibleForTesting
import io.circe._
import io.circe.parser._
import journal.Logger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class KafkaConsumerWrapper[T](settings: ConsumerSettings[String, String],
                       index: T => Future[Unit],
                       topic: String,
                       decoder: Decoder[T])
    extends Actor {

  private implicit val ec: ExecutionContext  = context.dispatcher
  private implicit val to: Timeout           = 30.seconds
  private implicit val mt: ActorMaterializer = ActorMaterializer()
  private implicit val ed: Decoder[T]        = decoder

  private val log = Logger[this.type]

  override def receive: Receive = {
    case value: String =>
      decode[T](value) match {
        case Right(event) =>
          log.debug(s"Received message: $value")
          index(event).onComplete {
            case Success(_) => ()
            case Failure(e) => log.error(s"Failed to index event: $event; skipping.", e)
          }
          sender ! Done
        // $COVERAGE-OFF$
        case Left(e) =>
          log.error(s"Failed to decode message: $value; skipping.", e)
          sender ! Done
        // $COVERAGE-ON$
      }
  }

  override def preStart(): Unit = {
    val done = Consumer
      .committableSource(settings, Subscriptions.topics(topic))
      .mapAsync(1) { msg =>
        val process = self ? msg.record.value
        process.map {
          case Done => msg.committableOffset
          case _ => throw new IllegalStateException(s"Unexpected response after processing message: $msg")
        }
      }
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
        batch.updated(elem)
      }
      .mapAsync(1)(_.commitScaladsl())
      .runWith(Sink.ignore)

    // $COVERAGE-OFF$
    done.onComplete {
      case Failure(e) =>
        log.error("Stream failed with an error, stopping the actor and restarting.", e)
        self ! PoisonPill
      case Success(_) =>
        log.warn("Kafka consumer stream ended gracefully, however this should not happen; restarting.")
        self ! PoisonPill
    }
    // $COVERAGE-ON$
  }

}

object KafkaConsumer {

  /**
    * Starts a Kafka consumer that reads messages from a particular ''topic'', decodes them into
    * events of type ''T'' and indexes them using the provided function.  The consumer stream is
    * wrapped in its own actor whose life-cycle is managed by a [[akka.pattern.BackoffSupervisor]] instance.
    *
    * @param settings an instance of [[akka.kafka.ConsumerSettings]]
    * @param index    the indexing function that is applied to received events
    * @param topic    the Kafka topic to read messages from
    * @param decoder  a Circe decoder to deserialize received messages
    * @param as       an implicitly available actor system
    * @tparam T       the event type
    * @return the supervisor actor handle
    * @note  Calling this method multiple times within the same actor system context will result in
    *        an [[akka.actor.InvalidActorNameException]] being thrown. You must stop any previously existing
    *        supervisor first.
    */
  def start[T](settings: ConsumerSettings[String, String],
               index: T => Future[Unit],
               topic: String,
               decoder: Decoder[T])(implicit as: ActorSystem): ActorRef = {
    val childProps = Props(classOf[KafkaConsumerWrapper[T]], settings, index, topic, decoder)
    val supervisor = as.actorOf(
      BackoffSupervisor.props(
        Backoff.onStop(
          childProps,
          childName = "kafka-stream-supervised-actor",
          minBackoff = 3.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.2
        )),
      name = "kafka-stream-supervisor"
    )

    supervisor
  }

  /**
    * Kills the managed actor that wraps the Kafka consumer, which should be re-spawned by its supervisor.
    *
    * @param supervisor the supervisor actor handle
    */
  @VisibleForTesting
  private[queue] def stop(supervisor: ActorRef)(implicit as: ActorSystem): Unit = {
    implicit val ec: ExecutionContext = as.dispatcher
    implicit val to: Timeout          = 30.seconds
    val child                         = supervisor ? GetCurrentChild
    child.foreach(_.asInstanceOf[CurrentChild].ref.get ! PoisonPill)
  }

}
