package ch.epfl.bluebrain.nexus.kg.auth.types

import ch.epfl.bluebrain.nexus.kg.auth.types.identity.Identity

/**
  * Type definition that is essentially a pair consisting of an ''identity'' and its associated ''permissions''.
  */
final case class AccessControl(identity: Identity, permissions: Set[Permission])
