package ch.epfl.bluebrain.nexus.kg.core.organizations

import ch.epfl.bluebrain.nexus.kg.core.Ref

/**
  * Reference data type to a specific organization revision.
  *
  * @param id  the unique identifier for the organization
  * @param rev a revision identifier for the organization
  */
final case class OrgRef(id: OrgId, rev: Long)

object OrgRef {
  final implicit def orgRef(ref: OrgRef): Ref[OrgId] = Ref(ref.id, ref.rev)
}
