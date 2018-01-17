package ch.epfl.bluebrain.nexus.kg.indexing.organizations

import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.StatusCodes
import cats.MonadError
import cats.instances.string._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticClientError
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent.{OrgCreated, OrgDeprecated, OrgUpdated}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgEvent, OrgId}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.jsonld.IndexJsonLdSupport._
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import io.circe.Json
import journal.Logger

/**
  * Organization incremental indexing logic that pushes data into an ElasticSearch indexer.
  *
  * @param client   the ElasticSearch client to use for communicating with the ElasticSearch indexer
  * @param contexts the context operation bundle
  * @param settings the indexing settings
  * @tparam F the monadic effect type
  */
class OrganizationEsIndexer[F[_]](client: ElasticClient[F],
                                  contexts: Contexts[F],
                                  settings: OrganizationEsIndexingSettings)(implicit F: MonadError[F, Throwable])
    extends Resources {

  private val log                                                      = Logger[this.type]
  private val OrganizationEsIndexingSettings(prefix, t, base, baseVoc) = settings

  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]   = Qualifier.configured[OrgId](base)
  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](baseVoc)

  private val revKey               = "rev".qualifyAsString
  private val deprecatedKey        = "deprecated".qualifyAsString
  private val orgName              = "name".qualifyAsString
  private lazy val indexJson: Json = jsonContentOf("/es-index.json", Map(quote("{{type}}") -> t))

  private def createIndexIfNotExist(id: OrgId): F[Unit] = {
    val index = id.toIndex(prefix)
    client.existsIndex(index).recoverWith {
      case ElasticClientError(StatusCodes.NotFound, _) => client.createIndex(index, indexJson)
      case other                                       => F.raiseError(other)
    }
  }

  /**
    * Indexes the event by pushing it's json ld representation into the ElasticSearch indexer while also updating the
    * existing content.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: OrgEvent): F[Unit] = event match {
    case OrgCreated(id, rev, m, value) =>
      log.debug(s"Indexing 'OrgCreated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(false))
      createIndexIfNotExist(event.id).flatMap { _ =>
        contexts
          .resolve(value removeKeys ("links"))
          .map(_ deepMerge meta)
          .flatMap { json =>
            val jsonWithMeta = json deepMerge Json.obj(createdAtTimeKey -> m.instant.jsonLd)
            client.create(event.id.toIndex(prefix), t, event.id.elasticId, jsonWithMeta)
          }
      }

    case OrgUpdated(id, rev, m, value) =>
      log.debug(s"Indexing 'OrgUpdated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(false))
      contexts
        .resolve(value removeKeys ("links"))
        .map(_ deepMerge meta)
        .flatMap { json =>
          client.update(event.id.toIndex(prefix), t, event.id.elasticId, Json.obj("doc" -> json))
        }

    case OrgDeprecated(id, rev, m) =>
      log.debug(s"Indexing 'OrgDeprecated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(true))
      client.update(event.id.toIndex(prefix), t, event.id.elasticId, Json.obj("doc" -> meta))
  }

  private def buildMeta(id: OrgId, rev: Long, meta: Meta, deprecated: Option[Boolean]): Json = {
    val sharedObj = Json.obj(
      idKey                    -> Json.fromString(id.qualifyAsString),
      revKey                   -> Json.fromLong(rev),
      orgName                  -> Json.fromString(id.id),
      updatedAtTimeKey         -> meta.instant.jsonLd,
      PrefixMapping.rdfTypeKey -> "Organization".qualify.jsonLd
    )

    val deprecatedObj = deprecated
      .map(v => Json.obj(deprecatedKey -> Json.fromBoolean(v)))
      .getOrElse(Json.obj())

    deprecatedObj deepMerge sharedObj
  }
}

object OrganizationEsIndexer {

  /**
    * Constructs a organization incremental indexer that pushes data into an ElasticSearch indexer.
    *
    * @param client   the ElasticSearch client to use for communicating with the ElasticSearch indexer
    * @param contexts the context operation bundle
    * @param settings the indexing settings
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](client: ElasticClient[F], contexts: Contexts[F], settings: OrganizationEsIndexingSettings)(
      implicit F: MonadError[F, Throwable]): OrganizationEsIndexer[F] =
    new OrganizationEsIndexer[F](client, contexts, settings)
}
