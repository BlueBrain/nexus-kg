package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Iri._

/**
  * A stable project reference.
  *
  * @param id The unique identifier for a project
  * @param organization The unique identifier for the project's organization
  */
final case class ProjectRef(id: AbsoluteIri, organization: OrganizationRef) {}

object ProjectRef {

  /**
    * @param id the project unique identifier
    * @return a new [[ProjectRef]] from the project's unique identifier
    */
  final def apply(id: AbsoluteIri): ProjectRef =
    ProjectRef(id, OrganizationRef(organization(id)))

  private def organization(id: AbsoluteIri): AbsoluteIri = {

    def deleteEnding(path: Path) =
      path match {
        case Segment(_, Slash(rest)) => rest
        case Segment(_, rest)        => rest
        case _                       => path
      }

    id match {
      case url: Url => url.copy(path = deleteEnding(url.path))
      case urn: Urn => urn.copy(nss = deleteEnding(urn.nss))
    }
  }

}
