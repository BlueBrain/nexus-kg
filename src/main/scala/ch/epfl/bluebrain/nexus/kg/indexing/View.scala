package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.UUID

import cats.MonadError
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticFailure}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidPayload, Unexpected}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.{DeprecatedId, RevisionedId}
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.encoder._
import io.circe.Json
import io.circe.parser._

import scala.util.Try
import scala.util.control.NonFatal

/**
  * Enumeration of view types.
  */
sealed trait View extends Product with Serializable {

  /**
    * @return a reference to the project that the view belongs to
    */
  def ref: ProjectRef

  /**
    * @return the user facing view id
    */
  def id: AbsoluteIri

  /**
    * @return the underlying uuid generated for this view
    */
  def uuid: String

  /**
    * @return the view revision
    */
  def rev: Long

  /**
    * @return the deprecation state of the view
    */
  def deprecated: Boolean

  /**
    * @return a generated name that uniquely identifies the view and its current revision
    */
  def name: String =
    s"${ref.id}_${uuid}_$rev"
}

object View {
  private val allowedMappingKeys: Set[String] = Set("dynamic_templates", "properties", "dynamic")

  private implicit class NodeEncoderResultSyntax[A](private val enc: NodeEncoder.EncoderResult[A]) extends AnyVal {
    def toInvalidPayloadEither(ref: Ref): Either[Rejection, A] =
      enc.left.map(err =>
        InvalidPayload(ref, s"The provided payload could not be mapped to a view due to '${err.message}'"))
  }

  /**
    * Attempts to transform the resource into a [[ch.epfl.bluebrain.nexus.kg.indexing.View]].
    *
    * @param res a materialized resource
    * @return Right(view) if the resource is compatible with a View, Left(rejection) otherwise
    */
  final def apply(res: ResourceV): Either[Rejection, View] = {
    val c          = res.value.graph.cursor(res.id.value)
    val uuidEither = c.downField(nxv.uuid).focus.as[UUID]

    def validMapping(mapping: Json): Boolean =
      mapping.asObject.map(_.keys.toSet.subsetOf(allowedMappingKeys)).getOrElse(false)

    def elastic(): Either[Rejection, View] =
      // format: off
      for {
        uuid          <- uuidEither.toInvalidPayloadEither(res.id.ref)
        mappingStr    <- c.downField(nxv.mapping).focus.as[String].toInvalidPayloadEither(res.id.ref)
        mapping       <- parse(mappingStr).left.map[Rejection](_ => InvalidPayload(res.id.ref, "mappings cannot be parsed into Json"))
        _             <- if (validMapping(mapping)) Right(()) else Left(InvalidPayload(res.id.ref, s"mappings should only contain some of the following keys: '${allowedMappingKeys.mkString(", ")}'"))
        schemas       = c.downField(nxv.resourceSchemas).values.asListOf[AbsoluteIri].map(_.toSet).getOrElse(Set.empty)
        tag           = c.downField(nxv.resourceTag).focus.as[String].toOption
        includeMeta   = c.downField(nxv.includeMetadata).focus.as[Boolean].getOrElse(false)
        sourceAsText  = c.downField(nxv.sourceAsText).focus.as[Boolean].getOrElse(false)
      } yield
        ElasticView(mapping, schemas, tag, includeMeta, sourceAsText, res.id.parent, res.id.value, uuid.toString.toLowerCase, res.rev, res.deprecated)
      // format: on

    def sparql(): Either[Rejection, View] =
      uuidEither
        .toInvalidPayloadEither(res.id.ref)
        .map(uuid => SparqlView(res.id.parent, res.id.value, uuid.toString.toLowerCase, res.rev, res.deprecated))

    if (Set(nxv.View.value, nxv.Alpha.value, nxv.ElasticView.value).subsetOf(res.types)) elastic()
    else if (Set(nxv.View.value, nxv.SparqlView.value).subsetOf(res.types)) sparql()
    else Left(InvalidPayload(res.id.ref, "The provided @type do not match any of the view types"))
  }

  /**
    * ElasticSearch specific view.
    *
    * @param mapping         the ElasticSearch mapping for the index
    * @param resourceSchemas set of schemas absolute iris used in the view. Indexing will be triggered only for
    *                        resources validated against any of those schemas
    * @param resourceTag     an optional tag. When present, indexing will be triggered only by resources tagged with the specified tag
    * @param includeMetadata flag to include or exclude metadata on the indexed Document
    * @param sourceAsText    flag to include or exclude the source Json as a blob
    *                        (if true, it will be included in the field '_original_source')
    * @param ref             a reference to the project that the view belongs to
    * @param id              the user facing view id
    * @param uuid            the underlying uuid generated for this view
    * @param rev             the view revision
    * @param deprecated      the deprecation state of the view
    */
  final case class ElasticView(
      mapping: Json,
      resourceSchemas: Set[AbsoluteIri],
      resourceTag: Option[String],
      includeMetadata: Boolean,
      sourceAsText: Boolean,
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: String,
      rev: Long,
      deprecated: Boolean
  ) extends View {

    /**
      * Generates the elasticSearch index
      *
      * @param config the [[ElasticConfig]]
      */
    def index(implicit config: ElasticConfig): String = s"${config.indexPrefix}_$name"

    /**
      * Attempts to Vi the index for the [[ElasticView]].
      *
      * @tparam F the effect type
      * @return ''Unit'' when the index was successfully created, a ''Rejection'' signaling the type of error
      *         when the index couldn't be created wrapped in an [[Either]]. The either is then wrapped in the
      *         effect type ''F''
      */
    def createIndex[F[_]](implicit elastic: ElasticClient[F],
                          config: ElasticConfig,
                          F: MonadError[F, Throwable]): F[Either[Rejection, Unit]] =
      elastic
        .createIndex(index, Json.obj("mappings" -> Json.obj(config.docType -> mapping)))
        .map[Either[Rejection, Unit]] {
          case true  => Right(())
          case false => Left(Unexpected("View mapping validation could not be performed"))
        }
        .recoverWith {
          case err: ElasticFailure => F.pure(Left(InvalidPayload(Ref(id), err.body)))
          case NonFatal(err) =>
            val msg = Try(err.getMessage).getOrElse("")
            F.pure(Left(Unexpected(s"View mapping validation could not be performed. Cause '$msg'")))
        }
  }

  /**
    * Sparql specific view.
    *
    * @param ref        a reference to the project that the view belongs to
    * @param id         the user facing view id
    * @param uuid       the underlying uuid generated for this view
    * @param rev        the view revision
    * @param deprecated the deprecation state of the view
    */
  final case class SparqlView(
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: String,
      rev: Long,
      deprecated: Boolean
  ) extends View

  final implicit val viewRevisionedId: RevisionedId[View] = RevisionedId(view => (view.id, view.rev))
  final implicit val viewDeprecatedId: DeprecatedId[View] = DeprecatedId(r => (r.id, r.deprecated))

}
