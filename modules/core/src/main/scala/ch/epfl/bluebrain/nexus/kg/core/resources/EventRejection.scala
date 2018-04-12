package ch.epfl.bluebrain.nexus.kg.core.resources

import ch.epfl.bluebrain.nexus.commons.types.Rejection

/**
  * Enumeration type for rejections returned when attempting to evaluate commands.
  */
sealed trait EventRejection extends Rejection
object EventRejection {

  /**
    * Signals the failure to perform a resource modification due to payload shape constraint violations.
    *
    * @param violations the collections of violations that have occurred
    */
  final case class ShapeConstraintViolations(violations: List[String]) extends EventRejection

  /**
    * Signals that a resource cannot be created because one with the same identifier already exists.
    */
  final case object ResourceAlreadyExists extends EventRejection

  /**
    * Signals that an operation on a resource cannot be performed due to the fact that the referenced resource does not exist.
    */
  final case object ResourceDoesNotExists extends EventRejection

  /**
    * Signals that an operation on a resource cannot be performed due to the fact that the referenced parent resource does not exist.
    */
  final case object ParentResourceDoesNotExists extends EventRejection

  /**
    * Signals that a resource update cannot be performed due its deprecation status.
    */
  final case object ResourceIsDeprecated extends EventRejection

  /**
    * Signals that a resource undeprecate cannot be performed due its deprecation status.
    */
  final case object ResourceIsNotDeprecated extends EventRejection

  /**
    * Signals that a resource update cannot be performed due to an incorrect revision provided.
    */
  final case object IncorrectRevisionProvided extends EventRejection

  /**
    * Signals the failure to perform a schema modification due to payload missing imports.
    *
    * @param imports the collections of imports that are not accepted
    */
  final case class MissingImportsViolation(imports: Set[String]) extends EventRejection

  /**
    * Signals the failure to perform a schema modification due to payload illegal imports.
    *
    * @param imports the collections of imports that are not accepted
    */
  final case class IllegalImportsViolation(imports: Set[String]) extends EventRejection

  /**
    * Signals the impossibility to unattach form a resource an attachment that does not exists.
    */
  final case object AttachmentDoesNotExists extends EventRejection

}
