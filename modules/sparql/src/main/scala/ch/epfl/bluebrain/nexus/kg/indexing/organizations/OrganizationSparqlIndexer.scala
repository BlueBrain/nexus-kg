package ch.epfl.bluebrain.nexus.kg.indexing.organizations

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.sparql.client.{PatchStrategy, SparqlClient}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.ld.JsonLdOps._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent.{OrgCreated, OrgDeprecated, OrgUpdated}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgEvent, OrgId}
import ch.epfl.bluebrain.nexus.kg.indexing.BaseSparqlIndexer
import io.circe.Json
import journal.Logger

/**
  * Organization incremental indexing logic that pushes data into an rdf triple store.
  *
  * @param client   the SPARQL client to use for communicating with the rdf triple store
  * @param contexts the context operation bundle
  * @param settings the indexing settings
  * @tparam F the monadic effect type
  */
class OrganizationSparqlIndexer[F[_]](
    client: SparqlClient[F],
    contexts: Contexts[F],
    settings: OrganizationSparqlIndexingSettings)(implicit F: MonadError[F, Throwable])
    extends BaseSparqlIndexer(settings.orgBase, settings.nexusVocBase) {

  private val log    = Logger[this.type]
  private val baseNs = settings.orgBaseNs

  /**
    * Indexes the event by pushing it's json ld representation into the rdf triple store while also updating the
    * existing triples.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: OrgEvent): F[Unit] = event match {
    case OrgCreated(id, rev, m, value) =>
      log.debug(s"Indexing 'OrgCreated' event for id '${id.show}'")
      contexts.resolve(value removeKeys "links").flatMap { resolved =>
        val meta = buildMeta(id, rev, m, deprecated = Some(false))
        val data = resolved deepMerge meta deepMerge Json.obj(createdAtTimeKey -> m.instant.jsonLd)
        client.replace(id qualifyWith baseNs, data)
      }

    case OrgUpdated(id, rev, m, value) =>
      log.debug(s"Indexing 'OrgUpdated' event for id '${id.show}'")
      contexts.resolve(value removeKeys "links").flatMap { resolved =>
        val meta     = buildMeta(id, rev, m, deprecated = Some(false))
        val data     = resolved deepMerge meta
        val strategy = PatchStrategy.removeButPredicates(Set(createdAtTimeKey))
        client.patch(id qualifyWith baseNs, data, strategy)
      }

    case OrgDeprecated(id, rev, m) =>
      log.debug(s"Indexing 'OrgDeprecated' event for id '${id.show}'")
      val meta     = buildMeta(id, rev, m, deprecated = Some(true))
      val strategy = PatchStrategy.removePredicates(Set(revKey, deprecatedKey, updatedAtTimeKey))
      client.patch(id qualifyWith baseNs, meta, strategy)
  }

  private def buildMeta(id: OrgId, rev: Long, meta: Meta, deprecated: Option[Boolean]): Json = {
    val sharedObj = Json.obj(
      idKey                    -> Json.fromString(id.qualifyAsString),
      revKey                   -> Json.fromLong(rev),
      nameKey                  -> Json.fromString(id.id),
      updatedAtTimeKey         -> meta.instant.jsonLd,
      PrefixMapping.rdfTypeKey -> "Organization".qualify.jsonLd
    )

    val deprecatedObj = deprecated
      .map(v => Json.obj(deprecatedKey -> Json.fromBoolean(v)))
      .getOrElse(Json.obj())

    deprecatedObj deepMerge sharedObj
  }
}

object OrganizationSparqlIndexer {

  /**
    * Constructs a organization incremental indexer that pushes data into an rdf triple store.
    *
    * @param client   the SPARQL client to use for communicating with the rdf triple store
    * @param contexts the context operation bundle
    * @param settings the indexing settings
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](client: SparqlClient[F], contexts: Contexts[F], settings: OrganizationSparqlIndexingSettings)(
      implicit F: MonadError[F, Throwable]): OrganizationSparqlIndexer[F] =
    new OrganizationSparqlIndexer[F](client, contexts, settings)
}
