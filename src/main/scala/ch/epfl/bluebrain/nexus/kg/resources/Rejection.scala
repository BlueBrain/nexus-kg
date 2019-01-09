package ch.epfl.bluebrain.nexus.kg.resources

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.shacl.topquadrant.ValidationReport
import ch.epfl.bluebrain.nexus.commons.types.Err
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.circe.JenaModel.JenaModelErr

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
    * Signals an attempt to interact with a resource that is expected to be a file resource but it isn't.
    *
    * @param ref a reference to the resource
    */
  final case class NotFileResource(ref: Ref) extends Rejection(s"Resource '${ref.show}' is not a file resource.")

  /**
    * Signals an attempt to perform a request with an invalid payload.
    *
    * @param ref a reference to the resource
    * @param reason the human readable reason for the rejection
    */
  final case class InvalidPayload(ref: Ref, reason: String)
      extends Rejection(s"Resource '${ref.show}' with invalid payload due to '$reason'.")

  /**
    * Signals an attempt to perform a request with an invalid JSON-LD payload.
    *
    * @param reason the human readable reason for the rejection
    */
  final case class InvalidJsonLD(reason: String) extends Rejection(s"Invalid payload due to '$reason'.")

  /**
    * Signals an attempt to interact with a resource that doesn't exist.
    *
    * @param ref a reference to the resource
    */
  final case class NotFound(ref: Ref) extends Rejection(s"Resource '${ref.show}' not found.")

  /**
    * Signals an attempt to interact with a project that doesn't have an organization.
    *
    * @param ref a reference to the resource
    */
  final case class OrganizationNotFound(ref: ProjectRef)
      extends Rejection(s"Project '${ref.show}' without an organization")

  /**
    * Signals the impossibility to resolve the project reference for project labels.
    *
    * @param labels the project labels were references were not found
    */
  final case class ProjectsNotFound(labels: Set[ProjectLabel])
      extends Rejection(s"Project references for labels '${labels.map(_.show).mkString(", ")}' not found.")

  /**
    * Signals the impossibility to resolve the labels for project references.
    *
    * @param projects the project references where labels were not found
    */
  final case class LabelsNotFound(projects: List[ProjectRef])
      extends Rejection(s"Labels for projects with ref '${projects.map(_.show).mkString(", ")}' not found.")

  /**
    * Signals an attempt to interact with a resource that belongs to a deprecated project.
    *
    * @param ref a reference to the project
    */
  final case class ProjectIsDeprecated(ref: ProjectLabel) extends Rejection(s"Project '${ref.show}' is deprecated.")

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

  /**
    * Signals the inability to convert the requested query parameter.
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class IllegalParameter(override val message: String) extends Rejection(message)

  /**
    * Signals that the logged organization does not have one of the provided identities
    *
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class InvalidIdentity(override val message: String) extends Rejection(message)

  /**
    * Signals the requirement of a parameter to be present
    *
    * @param message a human readable description of the cause
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class MissingParameter(override val message: String) extends Rejection(message)

  /**
    * Signals that the provided client URI does not match any service endpoint
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case object InvalidResourceIri extends Rejection("Provided IRI does not match any service endpoint")

  /**
    * Constructs a Rejection from a [[ch.epfl.bluebrain.nexus.rdf.circe.JenaModel.JenaModelErr]].
    *
    * @param error the error to be transformed
    */
  final def fromJenaModelErr(error: JenaModelErr): Rejection = error match {
    case JenaModelErr.InvalidJsonLD(message) => InvalidJsonLD(message)
    case JenaModelErr.Unexpected(message)    => Unexpected(message)
  }
}
