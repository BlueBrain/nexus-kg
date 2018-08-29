package ch.epfl.bluebrain.nexus.kg.service

import _root_.io.circe.generic.extras.Configuration
import _root_.io.circe.generic.extras.auto._
import _root_.io.circe.java8.time._
import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ProducerSettings
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextEvent
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent
import ch.epfl.bluebrain.nexus.service.kafka.KafkaPublisher
import ch.epfl.bluebrain.nexus.service.kafka.key.Key
import org.apache.kafka.common.serialization.StringSerializer

// $COVERAGE-OFF$
private class StartKafkaPublishers(topic: String, queryJournalPlugin: String)(implicit as: ActorSystem) {

  private implicit val config: Configuration           = Configuration.default.withDiscriminator("type")
  private implicit val instanceKey: Key[InstanceEvent] = Key.key(_.id.show)
  private implicit val contextKey: Key[ContextEvent]   = Key.key(_.id.show)
  private implicit val schemaKey: Key[SchemaEvent]     = Key.key(_.id.show)

  private val producerSettings = ProducerSettings(as, new StringSerializer, new StringSerializer)

  private def instances(): ActorRef = {
    KafkaPublisher.startTagStream[InstanceEvent]("instances-to-kafka",
                                                 queryJournalPlugin,
                                                 "instance",
                                                 "kafka-instance-publisher",
                                                 producerSettings,
                                                 topic)
  }

  private def contexts(): ActorRef = {
    KafkaPublisher.startTagStream[ContextEvent]("contexts-to-kafka",
                                                queryJournalPlugin,
                                                "context",
                                                "kafka-context-publisher",
                                                producerSettings,
                                                topic)
  }

  private def schemas(): ActorRef = {
    KafkaPublisher.startTagStream[SchemaEvent]("schemas-to-kafka",
                                               queryJournalPlugin,
                                               "schema",
                                               "kafka-schema-publisher",
                                               producerSettings,
                                               topic)
  }
}

object StartKafkaPublishers {

  /**
    * Starts Kafka publishers for the event migration.
    *
    * @param topic              the target Kafka topic
    * @param queryJournalPlugin the datastore query journal plugin id
    * @param as                 an implicitly available actor system
    */
  def apply(topic: String, queryJournalPlugin: String)(implicit as: ActorSystem): Unit = {
    val publishers = new StartKafkaPublishers(topic, queryJournalPlugin)
    publishers.contexts()
    publishers.schemas()
    publishers.instances()
    ()
  }
}
// $COVERAGE-ON$
