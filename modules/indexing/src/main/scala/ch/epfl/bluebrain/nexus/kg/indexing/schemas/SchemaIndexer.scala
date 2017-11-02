package ch.epfl.bluebrain.nexus.kg.indexing.schemas

import cats.MonadError
import cats.instances.string._
import cats.syntax.show._
import cats.syntax.functor._
import cats.syntax.flatMap._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaEvent, SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.jsonld.UriJsonLDSupport._
import ch.epfl.bluebrain.nexus.kg.indexing.query.PatchQuery
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import io.circe.Json
import journal.Logger

/**
  * Schema incremental indexing logic that pushes data into an rdf triple store.
  *
  * @param client   the SPARQL client to use for communicating with the rdf triple store
  * @param contexts the context operation bundle
  * @param settings the indexing settings
  * @tparam F       the monadic effect type
  */
class SchemaIndexer[F[_]](client: SparqlClient[F], contexts: Contexts[F], settings: SchemaIndexingSettings)(implicit F: MonadError[F, Throwable]) {

  private val log                                                  = Logger[this.type]
  private val SchemaIndexingSettings(index, base, baseNs, baseVoc) = settings

  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)
  private implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](base)
  private implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)
  private implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId]     = Qualifier.configured[SchemaId](base)
  private implicit val stringQualifier: ConfiguredQualifier[String]         = Qualifier.configured[String](baseVoc)

  private val revKey        = "rev".qualifyAsString
  private val deprecatedKey = "deprecated".qualifyAsString
  private val publishedKey  = "published".qualifyAsString
  private val orgKey        = "organization".qualifyAsString
  private val domainKey     = "domain".qualifyAsString
  private val nameKey       = "name".qualifyAsString
  private val versionKey    = "version".qualifyAsString

  /**
    * Indexes the event by pushing it's json ld representation into the rdf triple store while also updating the
    * existing triples.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: SchemaEvent): F[Unit] = event match {
    case SchemaCreated(id, rev, _, value) =>
      log.debug(s"Indexing 'SchemaCreated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, deprecated = Some(false), published = Some(false))
      contexts.expand(value)
        .map( _ deepMerge meta)
        .flatMap(client.createGraph(index, id qualifyWith baseNs, _))

    case SchemaUpdated(id, rev, _, value) =>
      log.debug(s"Indexing 'SchemaUpdated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, deprecated = Some(false), published = Some(false))
      contexts.expand(value)
        .map( _ deepMerge meta)
        .flatMap(client.replaceGraph(index, id qualifyWith baseNs, _))

    case SchemaPublished(id, rev, _) =>
      log.debug(s"Indexing 'SchemaPublished' event for id '${id.show}'")
      val meta        = buildMeta(id, rev, deprecated = None, published = Some(true))
      val removeQuery = PatchQuery(id, revKey, publishedKey)
      client.patchGraph(index, id qualifyWith baseNs, removeQuery, meta)

    case SchemaDeprecated(id, rev, _) =>
      log.debug(s"Indexing 'SchemaDeprecated' event for id '${id.show}'")
      val meta        = buildMeta(id, rev, deprecated = Some(true), published = None)
      val removeQuery = PatchQuery(id, revKey, deprecatedKey)
      client.patchGraph(index, id qualifyWith baseNs, removeQuery, meta)
  }

  private def buildMeta(id: SchemaId, rev: Long, deprecated: Option[Boolean], published: Option[Boolean]): Json = {
    val sharedObj = Json.obj(
      idKey          -> Json.fromString(id.qualifyAsString),
      revKey         -> Json.fromLong(rev),
      orgKey         -> id.domainId.orgId.qualify.jsonLd,
      domainKey      -> id.domainId.qualify.jsonLd,
      nameKey        -> Json.fromString(id.name),
      versionKey     -> Json.fromString(id.version.show),
      schemaGroupKey -> id.schemaName.qualify.jsonLd,
      rdfTypeKey     -> "Schema".qualify.jsonLd
    )

    val publishedObj = published
      .map(v => Json.obj(publishedKey -> Json.fromBoolean(v)))
      .getOrElse(Json.obj())

    val deprecatedObj = deprecated
      .map(v => Json.obj(deprecatedKey -> Json.fromBoolean(v)))
      .getOrElse(Json.obj())

    deprecatedObj deepMerge publishedObj deepMerge sharedObj
  }
}

object SchemaIndexer {

  /**
    * Constructs a schema incremental indexer that pushes data into an rdf triple store.
    *
    * @param client   the SPARQL client to use for communicating with the rdf triple store
    * @param contexts  the context operation bundle
    * @param settings the indexing settings
    * @tparam F       the monadic effect type
    */
  final def apply[F[_]](client: SparqlClient[F], contexts: Contexts[F], settings: SchemaIndexingSettings)(implicit F: MonadError[F, Throwable]): SchemaIndexer[F] =
    new SchemaIndexer[F](client, contexts, settings)
}
