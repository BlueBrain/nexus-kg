package ch.epfl.bluebrain.nexus.kg.core.organizations

import io.circe.Json

/**
  * Data type representing the current state of an organization.
  *
  * @param id         the unique identifier for the organization
  * @param rev        the current revision of the organization
  * @param value      the value of the organization as a json value
  * @param deprecated whether the organization is deprecated or not
  */
final case class Organization(id: OrgId, rev: Long, value: Json, deprecated: Boolean)
