package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

/**
  * A stable organization reference.
  *
  * @param id the underlying stable identifier for an organization
  */
final case class OrganizationRef(id: UUID)
