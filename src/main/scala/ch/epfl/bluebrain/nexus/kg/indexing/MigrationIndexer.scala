package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
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

private[indexing] class MigrationIndexer(repo: Repo[Task], topic: String, base: AbsoluteIri, projectRef: ProjectRef)(
    implicit as: ActorSystem) {

  private val log              = Logger[this.type]
  private val consumerSettings = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)

  private val resourceSchema = Ref(resourceSchemaUri)
  private val resourceTypes  = Set(nxv.Resource.value)
  private val schemaTypes    = Set(nxv.Resource.value, nxv.Schema.value)

  // $COVERAGE-OFF$
  private def run(): ActorRef = KafkaConsumer.start[v0.Event](consumerSettings, index, topic, s"$topic-migration")

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
      case InstanceCreated(id, _, meta, value) =>
        repo.create(toId(id), resourceSchema, resourceTypes, value, meta.instant)
      case ContextCreated(id, _, meta, value) =>
        repo.create(toId(id), resourceSchema, resourceTypes, value, meta.instant)
      case SchemaCreated(id, _, meta, value) =>
        repo.create(toId(id), resourceSchema, schemaTypes, value, meta.instant)

      case InstanceUpdated(id, rev, meta, value) => repo.update(toId(id), rev, resourceTypes, value, meta.instant)
      case ContextUpdated(id, rev, meta, value)  => repo.update(toId(id), rev, resourceTypes, value, meta.instant)
      case SchemaUpdated(id, rev, meta, value)   => repo.update(toId(id), rev, schemaTypes, value, meta.instant)

      case InstanceDeprecated(id, rev, meta) => repo.deprecate(toId(id), rev, meta.instant)
      case ContextDeprecated(id, rev, meta)  => repo.deprecate(toId(id), rev, meta.instant)
      case SchemaDeprecated(id, rev, meta)   => repo.deprecate(toId(id), rev, meta.instant)

      case InstanceAttachmentCreated(id, rev, meta, value) =>
        repo.unsafeAttach(toId(id), rev, value.toBinaryAttributes, meta.instant)
      case InstanceAttachmentRemoved(id, rev, meta) =>
        repo
          .get(toId(id))
          .toRight(NotFound(toId(event.id).ref))
          .flatMap { res => // v0 instances can have only one attachment
            if (res.attachments.size == 1) repo.unattach(res.id, rev, res.attachments.head.filename, meta.instant)
            else EitherT.fromEither[Task](Left(UnexpectedState(res.id.ref)))
          }

      case _: ContextPublished | _: SchemaPublished =>
        repo
          .get(toId(event.id))
          .toRight(NotFound(toId(event.id).ref))
          .flatMap(res => repo.update(res.id, event.rev, res.types, res.value, event.meta.instant))
    }
  }

  private def toId(id: String): Id[ProjectRef] = Id(projectRef, base + id)

}

object MigrationIndexer {

  /**
    * Starts an indexer that reads v0 events from Kafka and migrates them into the v1 resource repository.
    *
    * @param repo       the resource repository
    * @param topic      the Kafka topic to read events from
    * @param base       the base URI of the target project
    * @param projectRef the target project, where events will be reindexed
    */
  final def start(repo: Repo[Task], topic: String, base: AbsoluteIri, projectRef: ProjectRef)(
      implicit as: ActorSystem): Unit = {
    val indexer = new MigrationIndexer(repo, topic, base, projectRef)
    indexer.run()
    ()
  }
}
