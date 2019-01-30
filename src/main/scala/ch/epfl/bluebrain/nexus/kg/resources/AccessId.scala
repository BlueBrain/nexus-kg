package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.urlEncode
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.service.http.Path._
import ch.epfl.bluebrain.nexus.service.http.UriOps._

object AccessId {

  /**
    * Build an access id (the Uri from where to fetch the resource from the API)
    * E.g.: {base}/v1/resources/{account}/{project}/{schemaId}/{resourceId}
    * The {schemaId} and {resourceId} will be shorten when possible using the
    * available prefixMappings.
    *
    * @param resourceId the resource identifier
    * @param schemaId   the schema identifier
    */
  def apply(resourceId: AbsoluteIri, schemaId: AbsoluteIri)(implicit project: Project,
                                                            http: HttpConfig): AbsoluteIri = {

    def prefix(resource: String): AbsoluteIri =
      url"${http.publicUri.append(http.prefix / resource / project.organizationLabel / project.label)}".value

    def removeBase(iri: AbsoluteIri): Option[String] =
      if (iri.asString.startsWith(project.base.asString)) Some(iri.asString.stripPrefix(project.base.asString))
      else None

    def aliasOrCurieFor(iri: AbsoluteIri): String = {
      lazy val aliases = project.apiMappings.collectFirst {
        case (p, `iri`) => p
      }
      lazy val curies = project.apiMappings.collectFirst {
        case (p, ns) if iri.asString.startsWith(ns.asString) =>
          s"$p:${urlEncode(iri.asString.stripPrefix(ns.asString))}"
      }
      lazy val base = removeBase(iri)

      aliases orElse curies orElse base getOrElse urlEncode(iri.asString)
    }

    val shortResourceId = aliasOrCurieFor(resourceId)
    schemaId match {
      case `fileSchemaUri`          => prefix("files") + shortResourceId
      case `viewSchemaUri`          => prefix("views") + shortResourceId
      case `resolverSchemaUri`      => prefix("resolvers") + shortResourceId
      case `shaclSchemaUri`         => prefix("schemas") + shortResourceId
      case `unconstrainedSchemaUri` => prefix("resources") + "_" + shortResourceId
      case _                        => prefix("resources") + aliasOrCurieFor(schemaId) + shortResourceId
    }
  }

}
