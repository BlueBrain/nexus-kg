package ch.epfl.bluebrain.nexus.kg.resources

/**
  * A stable account reference.
  *
  * @param id the underlying stable identifier for an account
  */
final case class AccountRef(id: String) extends AnyVal
