package ch.epfl.bluebrain.nexus.kg.core

import java.io.ByteArrayInputStream

import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import cats.syntax.traverse._
import cats.{MonadError, Show}
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidatorErr.{CouldNotFindImports, IllegalImportDefinition}
import ch.epfl.bluebrain.nexus.commons.shacl.validator.{ImportResolver, ShaclSchema}
import io.circe.Json
import org.apache.jena.rdf.model.{ModelFactory, Property, RDFNode, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr}

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * An ''ImportResolver'' for a generic id of type ''Id'' and a generic resource of type ''Resource''.
  *
  * @param idLoader        function that allows dynamic resource lookup in the system
  * @param contextResolver function that allows dynamic context resolution
  * @param F               a MonadError typeclass instance for ''F[_]''
  * @tparam F        the monadic effect type
  * @tparam Id       the generic type representing the ''id'' to be imported
  * @tparam Resource the generic type representing the ''resource'' to be imported
  */
abstract class BaseImportResolver[F[_], Id: Show, Resource](
    idLoader: Id => F[Option[Resource]],
    contextResolver: Json => F[Json])(implicit F: MonadError[F, Throwable])
    extends ImportResolver[F] {

  private final lazy val owlImports: Property = ResourceFactory.createProperty("http://www.w3.org/2002/07/owl#imports")

  def idBaseUri: String
  def idBaseUriToIgnore: Set[String]
  private lazy val idBaseUriSlashed       = s"$idBaseUri/"
  private lazy val idBaseUriSlashedLength = idBaseUriSlashed.length

  def toId(str: String): Try[Id]

  def asJson(resource: Resource): Json

  def addJson(resource: Resource, json: Json): Resource

  override def apply(schema: ShaclSchema): F[Set[ShaclSchema]] = {
    def inner(loaded: Map[Id, ShaclSchema], toLoad: Set[Id]): F[Set[ShaclSchema]] = {
      val batch = toLoad.foldLeft(List.empty[F[(Id, Option[Resource])]]) {
        case (acc, id) if loaded.contains(id) => acc
        case (acc, id)                        => loadJson(id).map(opt => (id, opt)) :: acc
      }
      if (batch.isEmpty) F.pure(loaded.values.toSet)
      else {
        val value = batch.sequence[F, (Id, Option[Resource])] // aggregate List[F] into F[List]
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

  private def loadJson(id: Id): F[Option[Resource]] = {
    idLoader(id).flatMap {
      case Some(resource) => contextResolver(asJson(resource)).map(resolved => Some(addJson(resource, resolved)))
      case None           => F.pure(None)
    }
  }

  def checkImports(list: List[(Id, Option[Resource])]): Either[CouldNotFindImports, List[(Id, ShaclSchema)]] = {
    list.foldLeft[Either[CouldNotFindImports, List[(Id, ShaclSchema)]]](Right(Nil)) {
      case (Left(CouldNotFindImports(missing)), (id, None)) =>
        Left(CouldNotFindImports(missing + qualify(id)))
      case (l @ Left(_), _) =>
        l
      case (Right(_), (id, None)) =>
        Left(CouldNotFindImports(Set(qualify(id))))
      case (Right(acc), (id, Some(sch))) =>
        Right(id -> toShaclSchema(id, sch) :: acc)
    }
  }

  private def lookupImports(jsons: List[Json]): Either[IllegalImportDefinition, Set[Id]] = {
    jsons.foldLeft[Either[IllegalImportDefinition, Set[Id]]](Right(Set.empty)) {
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

  private def nodeToId(node: RDFNode): Either[Either[String, Unit], Id] =
    if (!node.isURIResource) Left(Left(node.toString))
    else {
      val uri = node.asResource().getURI
      if (!uri.startsWith(idBaseUriSlashed))
        if (idBaseUriToIgnore.exists(uri.startsWith)) Left(Right(()))
        else Left(Left(uri))
      else toId(uri.substring(idBaseUriSlashedLength)).toEither.left.map(_ => Left(uri))
    }

  private def lookupImports(json: Json): Either[IllegalImportDefinition, Set[Id]] = {

    val model = ModelFactory.createDefaultModel()
    RDFDataMgr.read(model, new ByteArrayInputStream(json.noSpaces.getBytes), Lang.JSONLD)
    val nodes = model.listObjectsOfProperty(owlImports).asScala.toList
    nodes.foldLeft[Either[IllegalImportDefinition, Set[Id]]](Right(Set.empty)) {
      case (Left(IllegalImportDefinition(values)), elem) =>
        nodeToId(elem) match {
          case Left(Left(value)) => Left(IllegalImportDefinition(values + value))
          case _                 => Left(IllegalImportDefinition(values))
        }
      case (Right(schemaIds), elem) =>
        nodeToId(elem) match {
          case Left(Left(value)) => Left(IllegalImportDefinition(Set(value)))
          case Left(Right(_))    => Right(schemaIds)
          case Right(id)         => Right(schemaIds + id)
        }
    }
  }

  def toShaclSchema(id: Id, resource: Resource): ShaclSchema = {
    val meta = Json.obj("@id" -> Json.fromString(s"$idBaseUri/${id.show}"))
    ShaclSchema(asJson(resource) deepMerge meta)
  }

  def qualify(id: Id): String =
    s"$idBaseUri/${id.show}"
}
