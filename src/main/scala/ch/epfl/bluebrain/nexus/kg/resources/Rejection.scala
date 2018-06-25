package ch.epfl.bluebrain.nexus.kg.resources

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.validation.Validator.ValidationReport
import ch.epfl.bluebrain.nexus.commons.types.Err
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * Enumeration of resource rejection types.
  *
  * @param msg a descriptive message of the rejection
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
sealed abstract class Rejection(val msg: String) extends Err(msg) with Product with Serializable

@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
object Rejection {

  /**
    * Signals an internal failure where the state of a resource is not the expected state.
    *
    * @param ref a reference to the resource
    */
  final case class UnexpectedState(ref: Ref) extends Rejection(s"Resource '${ref.show}' is in an unexpected state.")

  /**
    * Signals an attempt to interact with a resource that is deprecated.
    *
    * @param ref a reference to the resource
    */
  final case class IsDeprecated(ref: Ref) extends Rejection(s"Resource '${ref.show}' is deprecated.")

  /**
    * Signals an attempt to interact with a resource's attachment that doesn't exist.
    *
    * @param ref      a reference to the resource
    * @param filename a reference's attachment filename
    */
  final case class AttachmentNotFound(ref: Ref, filename: String)
      extends Rejection(s"Resource '${ref.show}' attachment with filename '$filename' does not exists.")

  /**
    * Signals an attempt to change the type of a resource (from a schema to something else or from something else to a schema).
    *
    * @param ref a reference to the resource
    */
  final case class UpdateSchemaTypes(ref: Ref)
      extends Rejection(s"Resource '${ref.show}' cannot change it's schema type.")

  /**
    * Signals an attempt to interact with a resource that doesn't exist.
    *
    * @param ref a reference to the resource
    */
  final case class NotFound(ref: Ref) extends Rejection(s"Resource '${ref.show}' not found.")

  /**
    * Signals an attempt to interact with a resource with an incorrect revision.
    *
    * @param ref a reference to the resource
    * @param rev the revision provided
    */
  final case class IncorrectRev(ref: Ref, rev: Long)
      extends Rejection(s"Resource '${ref.show}' with incorrect revision '$rev' provided.")

  /**
    * Signals a mismatch between a resource representation and its id.
    *
    * @param ref a reference to the resource
    */
  final case class IncorrectId(ref: Ref) extends Rejection(s"Expected id '${ref.show}' was not found in the payload")

  /**
    * Signals an attempt to create a resource with wrong types on it's payload.
    *
    * @param ref   a reference to the resource
    * @param types the payload types
    */
  final case class IncorrectTypes(ref: Ref, types: Set[AbsoluteIri])
      extends Rejection(s"Resource '${ref.show}' with incorrect payload types '$types'.")

  /**
    * Signals an attempt to create a resource that already exists.
    *
    * @param ref a reference to the resource
    */
  final case class AlreadyExists(ref: Ref) extends Rejection(s"Resource '${ref.show}' already exists.")

  /**
    * Signals that a resource has an illegal (transitive) context value.
    *
    * @param refs the import value stack
    */
  final case class IllegalContextValue(refs: List[Ref])
      extends Rejection(s"Resource '${refs.reverseMap(_.show).mkString(" -> ")}' has an illegal context value.")

  /**
    * Signals that the system is unable to select a primary node from a resource graph.
    */
  final case object UnableToSelectResourceId
      extends Rejection("Resource is not entity centric, unable to select primary node.")
  type UnableToSelectResourceId = UnableToSelectResourceId.type

  /**
    * Signals that a resource validation failed.
    *
    * @param schema a reference to the schema
    * @param report the validation report
    */
  final case class InvalidResource(schema: Ref, report: ValidationReport)
      extends Rejection(s"Resource failed to validate against the constraints defined by '${schema.show}'")

  /**
    * Signals the inability to connect to an underlying service to perform a request
    *
    * @param message a human readable description of the cause
    */
  final case class DownstreamServiceError(override val message: String) extends Rejection(message)

  /**
    * Signals an unexpected rejection
    *
    * @param message a human readable description of the cause
    */
  final case class Unexpected(override val message: String) extends Rejection(message)
}
