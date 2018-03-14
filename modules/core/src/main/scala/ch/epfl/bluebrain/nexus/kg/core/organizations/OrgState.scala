package ch.epfl.bluebrain.nexus.kg.core.organizations

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import io.circe.Json

/**
  * Enumeration type for possible states of an organization.
  */
sealed trait OrgState extends Product with Serializable

object OrgState {

  /**
    * Initial state of all organizations.
    */
  final case object Initial extends OrgState

  /**
    * State used for all organizations that have been created and later possibly updated.
    *
    * @param id         the unique identifier for the organization
    * @param rev        the last revision number
    * @param meta       the metadata associated to this organization
    * @param value      the current json value
    * @param deprecated the deprecation status
    */
  final case class Current(id: OrgId, rev: Long, meta: Meta, value: Json, deprecated: Boolean = false) extends OrgState

}
