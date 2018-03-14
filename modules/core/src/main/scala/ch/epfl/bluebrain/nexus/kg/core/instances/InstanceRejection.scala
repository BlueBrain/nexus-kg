package ch.epfl.bluebrain.nexus.kg.core.instances

import ch.epfl.bluebrain.nexus.commons.types.Rejection

/**
  * Enumeration type for rejections returned when attempting to evaluate commands.
  */
sealed trait InstanceRejection extends Rejection

object InstanceRejection {

  /**
    * Signals the failure to perform an instance modification due to payload shape constraint violations.
    *
    * @param violations the collections of violations that have occurred
    */
  final case class ShapeConstraintViolations(violations: List[String]) extends InstanceRejection

  /**
    * Signals that a instance cannot be created because one with the same identifier already exists.
    */
  final case object InstanceAlreadyExists extends InstanceRejection

  /**
    * Signals that an operation on an instance cannot be performed due to the fact that the referenced instance does not
    * exist.
    */
  final case object InstanceDoesNotExist extends InstanceRejection

  /**
    * Signals that an instance update cannot be performed due its deprecation status.
    */
  final case object InstanceIsDeprecated extends InstanceRejection

  /**
    * Signals that an instance update cannot be performed due to an incorrect revision provided.
    */
  final case object IncorrectRevisionProvided extends InstanceRejection

  /**
    * Signals that an instance attachment cannot be found
    */
  final case object AttachmentNotFound extends InstanceRejection

  /**
    * Signals that an instance attachment has a content length higher than allowed.
    *
    * @param limit the maximum content length allowed
    */
  final case class AttachmentLimitExceeded(limit: Long) extends InstanceRejection

}
