package ch.epfl.bluebrain.nexus.kg.core.schemas

import ch.epfl.bluebrain.nexus.kg.core.Rejection

/**
  * Enumeration type for rejections returned when attempting to evaluate commands.
  */
sealed trait SchemaRejection extends Rejection

object SchemaRejection {

  /**
    * Signals the failure to create a new schema due to an invalid ''id'' provided.
    *
    * @param id the provided identifier
    */
  final case class InvalidSchemaId(id: SchemaId) extends SchemaRejection

  /**
    * Signals the failure to perform a schema modification due to payload shape constraint violations.
    *
    * @param violations the collections of violations that have occurred
    */
  final case class ShapeConstraintViolations(violations: List[String]) extends SchemaRejection

  /**
    * Signals the failure to perform a schema modification due to payload missing imports.
    *
    * @param imports the collections of imports that are not accepted
    */
  final case class MissingImportsViolation(imports: Set[String]) extends SchemaRejection

  /**
    * Signals the failure to perform a schema modification due to payload illegal imports.
    *
    * @param imports the collections of imports that are not accepted
    */
  final case class IllegalImportsViolation(imports: Set[String]) extends SchemaRejection

  /**
    * Signals that a schema cannot be created because one with the same identifier already exists.
    */
  final case object SchemaAlreadyExists extends SchemaRejection

  /**
    * Signals that an operation on a schema cannot be performed due to the fact that the referenced schema does not
    * exist.
    */
  final case object SchemaDoesNotExist extends SchemaRejection

  /**
    * Signals that a schema update cannot be performed due its deprecation status.
    */
  final case object SchemaIsDeprecated extends SchemaRejection

  /**
    * Signals that an operation cannot be performed due to the fact that the schema is not published.
    */
  final case object SchemaIsNotPublished extends SchemaRejection

  /**
    * Signals that a schema update cannot be performed due its publish status.
    */
  final case object CannotUpdatePublished extends SchemaRejection

  /**
    * Signals that a schema update cannot be performed due to an incorrect revision provided.
    */
  final case object IncorrectRevisionProvided extends SchemaRejection

  /**
    * Signals that a schema cannot be un-published.
    */
  final case object CannotUnpublishSchema extends SchemaRejection

}