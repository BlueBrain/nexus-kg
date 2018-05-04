package ch.epfl.bluebrain.nexus.kg.core.access

/**
  * Enumeration type for access grants used to restrict operations on resources.
  *
  * @param name the access grant string value
  */
sealed abstract class Access(val name: String) extends Product with Serializable

object Access {

  /**
    * Read access grant
    */
  final case object Read extends Access("read")

  /**
    * Write access grant
    */
  final case object Write extends Access("write")

  /**
    * Create access grant
    */
  final case object Create extends Access("create")

  /**
    * Attach access grant
    */
  final case object Attach extends Access("attach")

  /**
    * Manage access grant
    */
  final case object Manage extends Access("manage")

  type Read   = Read.type
  type Write  = Write.type
  type Create = Create.type
  type Attach = Attach.type
  type Manage = Manage.type

}
