package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.{EitherT, OptionT}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.{BinaryAttributes, BinaryDescription}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json

/**
  * Resource operations.
  */
trait Resources[F[_]] {

  type RejOrResourceV = EitherT[F, Rejection, ResourceV]
  type RejOrResource  = EitherT[F, Rejection, Resource]
  type OptResource    = OptionT[F, Resource]

  /**
    * Creates a new resource attempting to extract the id from the source. If a primary node of the resulting graph
    * is found:
    * <ul>
    *   <li>if it's an iri then its value will be used</li>
    *   <li>if it's a bnode a new iri will be generated using the base value</li>
    * </ul>
    *
    * @param projectRef reference for the project in which the resource is going to be created.
    * @param base       base used to generate new ids.
    * @param schema     a schema reference that constrains the resource
    * @param source     the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(projectRef: ProjectRef, base: AbsoluteIri, schema: Ref, source: Json)(
      implicit identity: Identity): RejOrResource

  /**
    * Creates a new resource.
    *
    * @param id     the id of the resource
    * @param schema a schema reference that constrains the resource
    * @param source the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId, schema: Ref, source: Json)(implicit identity: Identity): RejOrResource

  /**
    * Creates a new resource.
    *
    * @param id     the id of the resource
    * @param schema a schema reference that constrains the resource
    * @param value  the resource representation in json-ld and graph formats
    */
  def create(id: ResId, schema: Ref, value: ResourceF.Value)(implicit identity: Identity): RejOrResource

  /**
    * Fetches the latest revision of a resource
    *
    * @param id        the id of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, schemaOpt: Option[Ref]): OptResource

  /**
    * Fetches the provided revision of a resource
    *
    * @param id        the id of the resource
    * @param rev       the revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, rev: Long, schemaOpt: Option[Ref]): OptResource

  /**
    * Fetches the provided tag of a resource
    *
    * @param id        the id of the resource
    * @param tag       the tag of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, tag: String, schemaOpt: Option[Ref]): OptResource

  /**
    * Updates an existing resource.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param source    the new source representation in json-ld format
    * @return either a rejection or the updated resource in the F context
    */
  def update(id: ResId, rev: Long, schemaOpt: Option[Ref], source: Json)(implicit identity: Identity): RejOrResource

  /**
    * Deprecates an existing resource
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate(id: ResId, rev: Long, schemaOpt: Option[Ref])(implicit identity: Identity): RejOrResource

  /**
    * Tags a resource. This operation aliases the provided ''targetRev'' with the  provided ''tag''.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param json      the json payload which contains the targetRev and the tag
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def tag(id: ResId, rev: Long, schemaOpt: Option[Ref], json: Json)(implicit identity: Identity): RejOrResource

  /**
    * Tags a resource. This operation aliases the provided ''targetRev'' with the  provided ''tag''.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param targetRev the revision that is being aliased with the provided ''tag''
    * @param tag       the tag of the alias for the provided ''rev''
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def tag(id: ResId, rev: Long, schemaOpt: Option[Ref], targetRev: Long, tag: String)(
      implicit identity: Identity): RejOrResource

  /**
    * Adds an attachment to a resource.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param attach    the attachment description metadata
    * @param source    the source of the attachment
    * @tparam In the storage input type
    * @return either a rejection or the new resource representation in the F context
    */
  def attach[In](id: ResId, rev: Long, schemaOpt: Option[Ref], attach: BinaryDescription, source: In)(
      implicit identity: Identity,
      store: AttachmentStore[F, In, _]): RejOrResource

  /**
    * Removes an attachment from a resource.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param filename  the attachment filename
    * @return either a rejection or the new resource representation in the F context
    */
  def unattach(id: ResId, rev: Long, schemaOpt: Option[Ref], filename: String)(
      implicit identity: Identity): RejOrResource

  /**
    * Attempts to stream the resource's attachment identified by the argument id and the filename.
    *
    * @param id        the id of the resource.
    * @param filename  the filename of the attachment
    * @param schemaOpt optional schema reference that constrains the resource
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def fetchAttachment[Out](id: ResId, schemaOpt: Option[Ref], filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)]

  /**
    * Attempts to stream the resource's attachment identified by the argument id, the revision and the filename.
    *
    * @param id        the id of the resource.
    * @param rev       the revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param filename  the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def fetchAttachment[Out](id: ResId, rev: Long, schemaOpt: Option[Ref], filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)]

  /**
    * Attempts to stream the resource's attachment identified by the argument id, the tag and the filename. The
    * tag is transformed into a revision value using the latest resource tag to revision mapping.
    *
    * @param id        the id of the resource.
    * @param tag       the tag of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param filename  the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def fetchAttachment[Out](id: ResId, tag: String, schemaOpt: Option[Ref], filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)]

  /**
    * Lists resources for the given project
    *
    * @param project        projects from which resources will be listed
    * @param deprecated     deprecation status of the resources
    * @param pagination     pagination options
    * @param tc             typed HTTP client
    * @return               search results in the F context
    */
  def list(project: ProjectRef, deprecated: Option[Boolean], pagination: Pagination)(
      implicit tc: HttpClient[F, QueryResults[AbsoluteIri]]): F[QueryResults[AbsoluteIri]]

  /**
    * Lists resources for the given project and schema
    *
    * @param project        projects from which resources will be listed
    * @param deprecated     deprecation status of the resources
    * @param schema         schema by which the resources are constrained
    * @param pagination     pagination options
    * @return               search results in the F context
    */
  def list(project: ProjectRef, deprecated: Option[Boolean], schema: AbsoluteIri, pagination: Pagination)(
      implicit tc: HttpClient[F, QueryResults[AbsoluteIri]],
  ): F[QueryResults[AbsoluteIri]]

  /**
    * Materializes a resource flattening its context and producing a raw graph. While flattening the context references
    * are transitively resolved.
    *
    * @param resource the resource to materialize
    */
  def materialize(resource: Resource): RejOrResourceV

  /**
    * Materializes a resource flattening its context and producing a graph that contains the additional type information
    * and the system generated metadata. While flattening the context references are transitively resolved.
    *
    * @param resource the resource to materialize
    */
  def materializeWithMeta(resource: Resource): RejOrResourceV

}
