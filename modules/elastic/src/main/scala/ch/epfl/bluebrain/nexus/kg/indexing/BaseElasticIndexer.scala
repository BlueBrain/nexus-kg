package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.regex.Pattern.quote

import cats.MonadError
import cats.instances.string._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.types.ConcurrentSetBuilder
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import io.circe.Json
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping.rdfTypeKey

/**
  * Base incremental indexing logic that pushes data into an ElasticSearch indexer.
  *
  * @param client   the ElasticSearch client to use for communicating with the ElasticSearch indexer
  * @param settings the indexing settings
  * @tparam F the monadic effect type
  */
private[indexing] abstract class BaseElasticIndexer[F[_]](client: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit F: MonadError[F, Throwable])
    extends Resources {
  val ElasticIndexingSettings(prefix, t, base, baseVoc) = settings

  implicit val orgIdQualifier: ConfiguredQualifier[OrgId]             = Qualifier.configured[OrgId](base)
  implicit val domainIdQualifier: ConfiguredQualifier[DomainId]       = Qualifier.configured[DomainId](base)
  implicit val contextNameQualifier: ConfiguredQualifier[ContextName] = Qualifier.configured[ContextName](base)
  implicit val contextIdQualifier: ConfiguredQualifier[ContextId]     = Qualifier.configured[ContextId](base)
  implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName]   = Qualifier.configured[SchemaName](base)
  implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId]       = Qualifier.configured[SchemaId](base)
  implicit val instanceIdQualifier: ConfiguredQualifier[InstanceId]   = Qualifier.configured[InstanceId](base)
  implicit val stringQualifier: ConfiguredQualifier[String]           = Qualifier.configured[String](baseVoc)

  val revKey: String        = "rev".qualifyAsString
  val deprecatedKey: String = "deprecated".qualifyAsString
  val publishedKey: String  = "published".qualifyAsString
  val nameKey: String       = "name".qualifyAsString
  val orgKey: String        = "organization".qualifyAsString
  val domainKey: String     = "domain".qualifyAsString
  val schemaKey: String     = "schema".qualifyAsString

  private lazy val indexJson: Json = jsonContentOf(
    "/es-index.json",
    Map(
      quote("{{type}}")         -> t,
      quote("{{rdfType}}")      -> rdfTypeKey,
      quote("{{contextGroup}}") -> "contextGroup".qualifyAsString,
      quote("{{organization}}") -> orgKey,
      quote("{{domain}}")       -> domainKey,
      quote("{{schema}}")       -> schemaKey,
      quote("{{schemaGroup}}")  -> "schemaGroup".qualifyAsString,
      quote("{{deprecated}}")   -> deprecatedKey,
      quote("{{published}}")    -> publishedKey
    )
  )
  private val indices = ConcurrentSetBuilder[String]()

  /**
    * Creates an index for the provided ''id'' when this does not exist on the cached ''indices''
    * and adds the created index to the cache
    *
    * @param id        the id for which the index is going to be created
    * @param converter the mapper between id and index
    * @tparam A the generic type of the id
    */
  def createIndexIfNotExist[A](id: A)(implicit converter: ElasticIndexerId[A]): F[Unit] = {
    val index = id.toIndex(prefix)
    if (!indices(index))
      client.createIndexIfNotExist(index, indexJson).map(_ => indices += index)
    else
      F.pure(())
  }
}
