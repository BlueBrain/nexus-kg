package ch.epfl.bluebrain.nexus.kg.auth.types

import ch.epfl.bluebrain.nexus.kg.auth.types.identity.Identity

/**
  * Type definition representing a mapping of identities to permissions for a specific resource.
  *
  * @param acl a set of [[AccessControl]] pairs.
  */
final case class AccessControlList(acl: Set[AccessControl]) {

  /**
    * @return a ''Map'' projection of the underlying pairs of identities and their permissions
    */
  def toMap: Map[Identity, Set[Permission]] = acl.map { case AccessControl(id, ps) => id -> ps }.toMap

}
