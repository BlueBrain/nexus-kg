package ch.epfl.bluebrain.nexus.kg.core.domains

import ch.epfl.bluebrain.nexus.kg.core.Ref

/**
  * Reference data type to a specific domain revision.
  *
  * @param id  the unique identifier for the domain
  * @param rev a revision identifier for the domain
  */
final case class DomainRef(id: DomainId, rev: Long)

object DomainRef {
  final implicit def domainRef(ref: DomainRef): Ref[DomainId] = Ref(ref.id, ref.rev)
}
