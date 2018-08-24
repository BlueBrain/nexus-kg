package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import cats.data.EitherT
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NotFound, UnexpectedState}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.v0.Event._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.service.kafka.KafkaConsumer
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future

private[indexing] class MigrationIndexer(repo: Repo[Task],
                                         topics: List[String],
                                         base: AbsoluteIri,
                                         projectRef: ProjectRef)(implicit as: ActorSystem) {

  private val log              = Logger[this.type]
  private val consumerSettings = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)

  private val resourceSchema = Ref(resourceSchemaUri)
  private val resourceTypes = Set(nxv.Resource.value)
  private val schemaTypes   = Set(nxv.Resource.value, nxv.Schema.value)

  // $COVERAGE-OFF$
  private def run(): Unit = {
    topics.foreach(topic => KafkaConsumer.start[v0.Event](consumerSettings, index, topic, s"$topic-migration"))
  }

  private def index(event: v0.Event): Future[Unit] = {
    process(event).value.map {
      case Right(res) => log.debug(s"Migrated v0 event '${event.id}' -> resource '${res.id}'")
      case Left(e)    => log.error(s"Couldn't migrate v0 event '${event.id}'", e)
    }.runAsync
  }
  // $COVERAGE-ON$

  private[indexing] def process(event: v0.Event): EitherT[Task, Rejection, Resource] = {
    implicit val author: Identity = event.meta.toIdentity
    event match {
      case InstanceCreated(id, _, _, value) => repo.create(toId(id), resourceSchema, resourceTypes, value)
      case ContextCreated(id, _, _, value)  => repo.create(toId(id), resourceSchema, resourceTypes, value)
      case SchemaCreated(id, _, _, value)   => repo.create(toId(id), resourceSchema, schemaTypes, value)

      case InstanceUpdated(id, rev, _, value) => repo.update(toId(id), rev, resourceTypes, value)
      case ContextUpdated(id, rev, _, value)  => repo.update(toId(id), rev, resourceTypes, value)
      case SchemaUpdated(id, rev, _, value)   => repo.update(toId(id), rev, schemaTypes, value)

      case InstanceDeprecated(id, rev, _) => repo.deprecate(toId(id), rev)
      case ContextDeprecated(id, rev, _)  => repo.deprecate(toId(id), rev)
      case SchemaDeprecated(id, rev, _)   => repo.deprecate(toId(id), rev)

      case InstanceAttachmentCreated(id, rev, _, value) =>
        repo.unsafeAttach(toId(id), rev, value.toBinaryAttributes)
      case InstanceAttachmentRemoved(id, rev, _) =>
        repo
          .get(toId(id))
          .toRight(NotFound(toId(event.id).ref))
          .flatMap { res => // v0 instances can have only one attachment
            if (res.attachments.size == 1) repo.unattach(res.id, rev, res.attachments.head.filename)
            else EitherT.fromEither[Task](Left(UnexpectedState(res.id.ref)))
          }

      case _: ContextPublished | _: SchemaPublished =>
        repo
          .get(toId(event.id))
          .toRight(NotFound(toId(event.id).ref))
          .flatMap(res => repo.update(res.id, event.rev, res.types, res.value))
    }
  }

  private def toId(id: String): Id[ProjectRef] = Id(projectRef, base + id)

}

object MigrationIndexer {

  /**
    * Starts an indexer that reads v0 events from Kafka and migrates them into the v1 resource repository.
    *
    * @param repo       the resource repository
    * @param topics     the Kafka topics to read events from
    * @param base       the base URI of the target project
    * @param projectRef the target project, where events will be reindexed
    */
  final def start(repo: Repo[Task], topics: List[String], base: AbsoluteIri, projectRef: ProjectRef)(
      implicit as: ActorSystem): Unit = {
    val indexer = new MigrationIndexer(repo, topics, base, projectRef)
    indexer.run()
  }
}
