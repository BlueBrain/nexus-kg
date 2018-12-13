package ch.epfl.bluebrain.nexus.kg.indexing.v0

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerSettings
import cats.data.EitherT
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.MigrationConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NotFound, UnexpectedState}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.v0.Event._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.service.kafka.KafkaConsumer
import io.circe.Json
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future
import scala.util.matching.Regex

private[v0] class MigrationIndexer(repo: Repo[Task],
                                   topic: String,
                                   base: AbsoluteIri,
                                   projectRef: ProjectRef,
                                   pattern: Regex)(implicit as: ActorSystem) {

  private val log              = Logger[this.type]
  private val consumerSettings = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)

  private val resourceSchema = Ref(resourceSchemaUri)
  private val shaclSchema    = Ref(shaclSchemaUri)
  private val resourceTypes  = Set(nxv.Resource.value)
  private val schemaTypes    = Set(nxv.Schema.value)

  // $COVERAGE-OFF$
  private def run(): ActorRef = KafkaConsumer.start[v0.Event](consumerSettings, index, topic, s"$topic-migration")

  private def index(event: v0.Event): Future[Unit] = event.id match {
    case pattern() =>
      process(event).value.map {
        case Right(res) => log.debug(s"Migrated v0 event '${event.id}' -> resource '${res.id}'")
        case Left(e)    => log.error(s"Couldn't migrate v0 event '${event.id}'", e)
      }.runToFuture
    case _ => Future.unit
  }
  // $COVERAGE-ON$

  private[v0] def process(event: v0.Event): EitherT[Task, Rejection, Resource] = {
    implicit val author: Identity = event.meta.toIdentity
    event match {
      case InstanceCreated(id, _, meta, value) =>
        repo.create(toId(id), extractSchema(id), extractTypes(value), value, meta.instant)
      case ContextCreated(id, _, meta, value) =>
        repo.create(toId(id), resourceSchema, resourceTypes, value, meta.instant)
      case SchemaCreated(id, _, meta, value) =>
        repo.create(toId(id), shaclSchema, schemaTypes, value, meta.instant)

      case InstanceUpdated(id, rev, meta, value) => repo.update(toId(id), rev, extractTypes(value), value, meta.instant)
      case ContextUpdated(id, rev, meta, value)  => repo.update(toId(id), rev, resourceTypes, value, meta.instant)
      case SchemaUpdated(id, rev, meta, value)   => repo.update(toId(id), rev, schemaTypes, value, meta.instant)

      case InstanceDeprecated(id, rev, meta) => repo.deprecate(toId(id), rev, meta.instant)
      case ContextDeprecated(id, rev, meta)  => repo.deprecate(toId(id), rev, meta.instant)
      case SchemaDeprecated(id, rev, meta)   => repo.deprecate(toId(id), rev, meta.instant)

      case InstanceAttachmentCreated(id, rev, meta, value) =>
        repo.attachFromMetadata(toId(id), rev, value.toBinaryAttributes, meta.instant)
      case InstanceAttachmentRemoved(id, rev, meta) =>
        repo
          .get(toId(id))
          .toRight(NotFound(toId(event.id).ref))
          .flatMap { res =>
            res.attachments.toList match { // v0 instances can have only one attachment
              case head :: Nil => repo.unattach(res.id, rev, head.filename, meta.instant)
              case _           => EitherT.fromEither[Task](Left(UnexpectedState(res.id.ref)))
            }
          }

      case _: ContextPublished | _: SchemaPublished =>
        for {
          res     <- repo.get(toId(event.id), event.rev).toRight(NotFound(toId(event.id).ref))
          updated <- repo.update(res.id, res.rev, res.types, res.value, event.meta.instant)
          tagged  <- repo.tag(updated.id, updated.rev, updated.rev, "published", event.meta.instant)
        } yield tagged
    }
  }

  private[v0] def toId(id: String): Id[ProjectRef] = Id(projectRef, buildId(id))

  private def extractTypes(value: Json): Set[AbsoluteIri] = {
    val types = value.asObject
      .flatMap(_.apply("@type"))
      .map(tpe => tpe.asString.toList ++ tpe.asArray.toList.flatMap(_.flatMap(_.asString))) match {
      case Some(tpes) => tpes.flatMap(Iri.absolute(_).toSeq)
      case None       => Seq.empty[AbsoluteIri]
    }

    if (types.isEmpty) resourceTypes
    else types.toSet
  }

  private def extractSchema(id: String): Ref = {
    val slash = id.lastIndexOf('/')
    if (slash < 0) resourceSchema
    else Ref(buildId(id.substring(0, slash)))
  }

  private def buildId(id: String): AbsoluteIri =
    id.split('/').foldLeft(base)((uri, segment) => uri + segment)
}

object MigrationIndexer {

  /**
    * Starts an indexer that reads v0 events from Kafka and migrates them into the v1 resource repository.
    *
    * @param repo   the resource repository
    * @param config the migration configuration settings
    */
  final def start(repo: Repo[Task], config: MigrationConfig)(implicit as: ActorSystem): Unit = {
    val indexer = new MigrationIndexer(repo, config.topic, config.baseUri, config.projectRef, config.pattern)
    indexer.run()
    ()
  }
}
