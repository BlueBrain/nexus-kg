package ch.epfl.bluebrain.nexus.kg.indexing

import akka.http.scaladsl.model.Uri
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}

/**
  * Base incremental indexing logic that pushes data into an rdf triple store.
  *
  * @param base    the application base uri for operating on resource
  * @param baseVoc the nexus core vocabulary base
  */
private[indexing] abstract class BaseSparqlIndexer(base: Uri, baseVoc: Uri) extends Resources {

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
  val publishedKey          = "published".qualifyAsString
  val nameKey: String       = "name".qualifyAsString
  val orgKey: String        = "organization".qualifyAsString
  val domainKey: String     = "domain".qualifyAsString
  val schemaKey: String     = "schema".qualifyAsString
}
