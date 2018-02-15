package ch.epfl.bluebrain.nexus.kg.service.types

trait Revisioned {

  /**
    * @return the revision number
    */
  def rev: Long
}
