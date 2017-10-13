package ch.epfl.bluebrain.nexus.kg.core.schemas

import java.io.ByteArrayInputStream

import cats.MonadError
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import cats.syntax.traverse._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidatorErr.{CouldNotFindImports, IllegalImportDefinition}
import ch.epfl.bluebrain.nexus.commons.shacl.validator.{ImportResolver, ShaclSchema}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaImportResolver.owlImports
import io.circe.Json
import org.apache.jena.rdf.model.{ModelFactory, Property, RDFNode, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr}

import scala.collection.JavaConverters._

/**
  * A transitive ''ImportResolver'' implementation that ensures schema imports are known uris within a published state.
  *
  * @param baseUri      the base uri of the system
  * @param schemaLoader function that allows dynamic schema lookup in the system
  * @param F            a MonadError typeclass instance for ''F[_]''
  * @tparam F           the monadic effect type
  */
class SchemaImportResolver[F[_]](baseUri: String, schemaLoader: SchemaId => F[Option[Schema]])(
    implicit F: MonadError[F, Throwable])
    extends ImportResolver[F] {

  private val schemaBaseUri              = s"$baseUri/schemas"
  private val schemaBaseUriSlashed       = s"$schemaBaseUri/"
  private val schemaBaseUriSlashedLength = schemaBaseUriSlashed.length

  override def apply(schema: ShaclSchema): F[Set[ShaclSchema]] = {
    def inner(loaded: Map[SchemaId, ShaclSchema], toLoad: Set[SchemaId]): F[Set[ShaclSchema]] = {
      val batch = toLoad.foldLeft(List.empty[F[(SchemaId, Option[Schema])]]) {
        case (acc, id) if loaded.contains(id) => acc
        case (acc, id)                        => schemaLoader(id).map(opt => (id, opt)) :: acc
      }
      if (batch.isEmpty) F.pure(loaded.values.toSet)
      else {
        val value = batch.sequence[F, (SchemaId, Option[Schema])] // aggregate List[F] into F[List]
        value.flatMap { list =>
          checkImports(list) match {
            case Left(missing) => F.raiseError(missing)
            case Right(newBatch) =>
              lookupImports(newBatch.map(_._2.value)) match {
                case Left(err)  => F.raiseError(err)
                case Right(ids) => inner(loaded ++ newBatch, ids)
              }
          }
        }
      }
    }

    lookupImports(schema.value) match {
      case Left(ill)  => F.raiseError(ill)
      case Right(ids) => inner(Map.empty, ids)
    }
  }

  private def checkImports(
      list: List[(SchemaId, Option[Schema])]): Either[CouldNotFindImports, List[(SchemaId, ShaclSchema)]] = {
    list.foldLeft[Either[CouldNotFindImports, List[(SchemaId, ShaclSchema)]]](Right(Nil)) {
      case (Left(CouldNotFindImports(missing)), (id, None)) => Left(CouldNotFindImports(missing + qualify(id)))
      case (Left(CouldNotFindImports(missing)), (id, Some(sch))) if !sch.published =>
        Left(CouldNotFindImports(missing + qualify(id)))
      case (l @ Left(_), _)                              => l
      case (Right(_), (id, None))                        => Left(CouldNotFindImports(Set(qualify(id))))
      case (Right(_), (id, Some(sch))) if !sch.published => Left(CouldNotFindImports(Set(qualify(id))))
      case (Right(acc), (id, Some(sch)))                 => Right(id -> toShaclSchema(sch) :: acc)
    }
  }

  private def lookupImports(jsons: List[Json]): Either[IllegalImportDefinition, Set[SchemaId]] = {
    jsons.foldLeft[Either[IllegalImportDefinition, Set[SchemaId]]](Right(Set.empty)) {
      case (l @ Left(IllegalImportDefinition(ill)), elem) =>
        lookupImports(elem) match {
          case Left(IllegalImportDefinition(extra)) => Left(IllegalImportDefinition(ill ++ extra))
          case Right(_)                             => l
        }
      case (Right(acc), elem) =>
        lookupImports(elem) match {
          case l @ Left(_) => l
          case Right(ids)  => Right(acc ++ ids)
        }
    }
  }

  private def lookupImports(json: Json): Either[IllegalImportDefinition, Set[SchemaId]] = {
    def toSchemaId(node: RDFNode): Either[String, SchemaId] = {
      if (!node.isURIResource) Left(node.toString)
      else {
        val uri = node.asResource().getURI
        if (!uri.startsWith(schemaBaseUriSlashed)) Left(uri)
        else SchemaId(uri.substring(schemaBaseUriSlashedLength)).toEither.left.map(_ => uri)
      }
    }

    val model = ModelFactory.createDefaultModel()
    RDFDataMgr.read(model, new ByteArrayInputStream(json.noSpaces.getBytes), Lang.JSONLD)
    val nodes = model.listObjectsOfProperty(owlImports).asScala.toList
    nodes.foldLeft[Either[IllegalImportDefinition, Set[SchemaId]]](Right(Set.empty)) {
      case (Left(IllegalImportDefinition(values)), elem) =>
        toSchemaId(elem) match {
          case Left(value) => Left(IllegalImportDefinition(values + value))
          case Right(_)    => Left(IllegalImportDefinition(values))
        }
      case (Right(schemaIds), elem) =>
        toSchemaId(elem) match {
          case Left(value) => Left(IllegalImportDefinition(Set(value)))
          case Right(id)   => Right(schemaIds + id)
        }
    }
  }

  private def toShaclSchema(schema: Schema): ShaclSchema = {
    val meta = Json.obj("@id" -> Json.fromString(s"$schemaBaseUri/${schema.id.show}"))
    ShaclSchema(schema.value.deepMerge(meta))
  }

  private def qualify(id: SchemaId): String =
    s"$schemaBaseUri/${id.show}"
}

object SchemaImportResolver {
  private final lazy val owlImports: Property = ResourceFactory.createProperty("http://www.w3.org/2002/07/owl#imports")

  /**
    * Constructs a transitive ''ImportResolver'' that ensures schema imports are known uris within a published state.
    *
    * @param baseUri      the base uri of the system
    * @param schemaLoader function that allows dynamic schema lookup in the system
    * @param F            a MonadError typeclass instance for ''F[_]''
    * @tparam F           the monadic effect type
    */
  final def apply[F[_]](baseUri: String, schemaLoader: SchemaId => F[Option[Schema]])(
      implicit F: MonadError[F, Throwable]): SchemaImportResolver[F] =
    new SchemaImportResolver[F](baseUri, schemaLoader)
}
