package ch.epfl.bluebrain.nexus.kg.resources

import java.time.Instant

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * A resource representation.
  *
  * @param id         the unique identifier of the resource
  * @param rev        the revision of the resource
  * @param types      the collection of known types of this resource
  * @param deprecated whether the resource is deprecated of not
  * @param tags       the collection of tag names to revisions of the resource
  * @param created    the instant when this resource was created
  * @param updated    the last instant when this resource was updated
  * @param createdBy  the identity that created this resource
  * @param updatedBy  the last identity that updated this resource
  * @param schema     the schema that this resource conforms to
  * @param value      the resource value
  * @tparam P the parent type of the resource identifier
  * @tparam S the schema type
  * @tparam A the resource value type
  */
final case class ResourceF[P, S, A](
    id: Id[P],
    rev: Long,
    types: Set[AbsoluteIri],
    deprecated: Boolean,
    tags: Map[String, Long],
    created: Instant,
    updated: Instant,
    createdBy: Identity,
    updatedBy: Identity,
    schema: S,
    value: A
)
