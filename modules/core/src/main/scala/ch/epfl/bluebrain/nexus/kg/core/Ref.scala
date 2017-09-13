package ch.epfl.bluebrain.nexus.kg.core

/**
  * Defines the signature for the common methods present on ''ref'' case classes.
  *
  * @tparam Id the id type
  */
trait Ref[Id] {
  def id: Id

  def rev: Long
}

object Ref {

  /**
    * Constructs a ''ref'' given an ''id'' and a ''rev''.
    *
    * @param identity a unique identifier
    * @param revision a revision identifier
    * @tparam Id the identifier type
    * @return an instance of a ''ref''
    */
  final def apply[Id](identity: Id, revision: Long) = new Ref[Id] {
    override def id = identity

    override def rev = revision
  }
}
