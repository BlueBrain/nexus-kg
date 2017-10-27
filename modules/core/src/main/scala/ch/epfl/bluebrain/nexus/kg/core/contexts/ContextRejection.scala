package ch.epfl.bluebrain.nexus.kg.core.contexts

import ch.epfl.bluebrain.nexus.commons.types.Rejection

/**
  * Enumeration type for rejections returned when attempting to evaluate commands.
  */
sealed trait ContextRejection extends Rejection

object ContextRejection {

  /**
    * Signals the failure to create a new context due to an invalid ''id'' provided.
    *
    * @param id the provided identifier
    */
  final case class InvalidContextId(id: ContextId) extends ContextRejection

  /**
    * Signals the failure to perform a context modification due to payload shape constraint violations.
    *
    * @param violations the collections of violations that have occurred
    */
  final case class ShapeConstraintViolations(violations: List[String]) extends ContextRejection

  /**
    * Signals the failure to perform a context modification due to payload illegal imports.
    *
    * @param imports the collections of imports that are not accepted
    */
  final case class IllegalImportsViolation(imports: Set[String]) extends ContextRejection

  /**
    * Signals that a context cannot be created because one with the same identifier already exists.
    */
  final case object ContextAlreadyExists extends ContextRejection

  /**
    * Signals that an operation on a context cannot be performed due to the fact that the referenced context does not
    * exist.
    */
  final case object ContextDoesNotExist extends ContextRejection

  /**
    * Signals that a context update cannot be performed due its deprecation status.
    */
  final case object ContextIsDeprecated extends ContextRejection

  /**
    * Signals that an operation cannot be performed due to the fact that the context is not published.
    */
  final case object ContextIsNotPublished extends ContextRejection

  /**
    * Signals that a context update cannot be performed due its publish status.
    */
  final case object CannotUpdatePublished extends ContextRejection

  /**
    * Signals that a context update cannot be performed due to an incorrect revision provided.
    */
  final case object IncorrectRevisionProvided extends ContextRejection

  /**
    * Signals that a context cannot be un-published.
    */
  final case object CannotUnpublishContext extends ContextRejection

}