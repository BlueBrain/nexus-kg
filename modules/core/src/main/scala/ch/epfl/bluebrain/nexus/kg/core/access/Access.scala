package ch.epfl.bluebrain.nexus.kg.core.access

/**
  * Enumeration type for access grants used to restrict operations on resources.
  */
sealed trait Access extends Product with Serializable

object Access {

  /**
    * Read access grant
    */
  final case object Read extends Access

  /**
    * Write access grant
    */
  final case object Write extends Access

  /**
    * Create access grant
    */
  final case object Create extends Access

  /**
    * Attach access grant
    */
  final case object Attach extends Access

  /**
    * Manage access grant
    */
  final case object Manage extends Access

  type Read   = Read.type
  type Write  = Write.type
  type Create = Create.type
  type Attach = Attach.type
  type Manage = Manage.type
}
