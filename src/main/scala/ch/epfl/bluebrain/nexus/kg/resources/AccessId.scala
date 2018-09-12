package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.service.http.Path._
import ch.epfl.bluebrain.nexus.service.http.UriOps._
import ch.epfl.bluebrain.nexus.kg.urlEncode

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
  def apply(resourceId: AbsoluteIri, schemaId: AbsoluteIri)(implicit wrapped: LabeledProject,
                                                            http: HttpConfig): AbsoluteIri = {

    def prefix(resource: String): AbsoluteIri =
      url"${http.publicUri.append(http.prefix / resource / wrapped.label.account / wrapped.label.value)}".value

    def aliasOrCurieFor(iri: AbsoluteIri): String =
      (wrapped.project.prefixMappings.collectFirst {
        case (p, `iri`) => p
      } orElse
        wrapped.project.prefixMappings.collectFirst {
          case (p, ns) if iri.asString.startsWith(ns.asString) => s"$p:${urlEncode(iri.asString.stripPrefix(ns.asString))}"
        }).getOrElse(urlEncode(iri.asString))

    val shortResourceId = aliasOrCurieFor(resourceId)
    schemaId match {
      case `viewSchemaUri`     => prefix("views") + shortResourceId
      case `resolverSchemaUri` => prefix("resolvers") + shortResourceId
      case `shaclSchemaUri`    => prefix("schemas") + shortResourceId
      case _                   => prefix("resources") + aliasOrCurieFor(schemaId) + shortResourceId
    }
  }

}
