package ch.epfl.bluebrain.nexus.kg.service.queue

import java.util.concurrent.TimeUnit.SECONDS

import akka.actor.Actor.emptyBehavior
import akka.actor._
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch, GroupTopicPartition}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.pattern.BackoffSupervisor.GetCurrentChild
import akka.pattern.{Backoff, BackoffSupervisor, ask}
import akka.persistence.query.Sequence
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.commons.service.persistence.IndexFailuresLog
import ch.epfl.bluebrain.nexus.commons.service.retryer.RetryOps._
import ch.epfl.bluebrain.nexus.commons.service.retryer.RetryStrategy.{Backoff => BackoffStrategy}
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

  private implicit val as: ActorSystem       = context.system
  private implicit val ec: ExecutionContext  = as.dispatcher
  private implicit val mt: ActorMaterializer = ActorMaterializer()
  private implicit val ed: Decoder[T]        = decoder

  private val log     = Logger[this.type]
  private val config  = context.system.settings.config.getConfig("indexing.retry")
  private val retries = config.getInt("max-count")
  private val strategy =
    BackoffStrategy(Duration(config.getDuration("max-duration", SECONDS), SECONDS), config.getDouble("random-factor"))
  private val failures = IndexFailuresLog("acls")

  override val receive: Receive = emptyBehavior

  override def preStart(): Unit = {
    val done = Consumer
      .committableSource(settings, Subscriptions.topics(topic))
      .mapAsync(1) { msg =>
        process(msg)
          .recover { case e => log.error(s"Unexpected failure while processing message: $msg", e) }
          .map(_ => msg.committableOffset)
      }
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
        batch.updated(elem)
      }
      .mapAsync(1)(_.commitScaladsl())
      .runWith(Sink.ignore)

    done.onComplete {
      case Failure(e) =>
        log.error("Stream failed with an error, stopping the actor and restarting.", e)
        kill()
      // $COVERAGE-OFF$
      case Success(_) =>
        log.warn("Kafka consumer stream ended gracefully, however this should not happen; restarting.")
        kill()
      // $COVERAGE-ON$
    }
  }

  private[queue] def kill(): Unit = self ! PoisonPill

  private def process(msg: CommittableMessage[String, String]): Future[Unit] = {
    val value = msg.record.value
    decode[T](value) match {
      case Right(event) =>
        log.debug(s"Received message: $value")
        (() => index(event))
          .retry(retries)(strategy)
          .recoverWith {
            // $COVERAGE-OFF$
            case e =>
              log.error(s"Failed to index event: $event; skipping.", e)
              storeFailure(msg)
          }
      case Left(e) =>
        log.error(s"Failed to decode message: $value; skipping.", e)
        storeFailure(msg)
      // $COVERAGE-ON$
    }
  }

  private def storeFailure(msg: CommittableMessage[String, String]): Future[Unit] = {
    // $COVERAGE-OFF$
    val offset                                       = Sequence(msg.committableOffset.partitionOffset.offset)
    val GroupTopicPartition(group, topic, partition) = msg.committableOffset.partitionOffset.key
    failures.storeEvent(s"$group-$topic-$partition", offset, msg.record.value)
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
  private[queue] def stop(supervisor: ActorRef)(implicit as: ActorSystem): Unit = {
    implicit val ec: ExecutionContext = as.dispatcher
    implicit val to: Timeout          = 30.seconds
    (supervisor ? GetCurrentChild).mapTo[KafkaConsumerWrapper[_]].foreach(_.kill())
  }

}
