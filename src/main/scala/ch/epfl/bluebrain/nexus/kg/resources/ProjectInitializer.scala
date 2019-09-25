package ch.epfl.bluebrain.nexus.kg.resources

import akka.actor.ActorSystem
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller}
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.async.{ProjectAttributesCoordinator, ProjectViewCoordinator}
import ch.epfl.bluebrain.nexus.kg.cache.Caches
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticSearchView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.InProjectResolver
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.ResourceAlreadyExists
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.DiskStorage
import ch.epfl.bluebrain.nexus.kg.storage.StorageEncoder._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._
import io.circe.Json
import journal.Logger

class ProjectInitializer[F[_]: Timer](
    storages: Storages[F],
    views: Views[F],
    resolvers: Resolvers[F],
    viewCoordinator: ProjectViewCoordinator[F],
    fileAttributesCoordinator: ProjectAttributesCoordinator[F]
)(implicit F: Effect[F], cache: Caches[F], config: AppConfig, as: ActorSystem) {
  private val logger      = Logger[this.type]
  private val revK        = nxv.rev.prefix
  private val deprecatedK = nxv.deprecated.prefix
  private val algorithmK  = nxv.algorithm.prefix

  private implicit val retry: Retry[F, KgError] = {
    import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
    Retry[F, KgError](config.keyValueStore.indexing.retry.retryStrategy)
  }

  private val createdOrExists: PartialFunction[Either[Rejection, Resource], Either[ResourceAlreadyExists, Resource]] = {
    case Left(exists: ResourceAlreadyExists) => Left(exists)
    case Right(value)                        => Right(value)
  }

  /**
    * Set-up the necessary elements in order for a project to be fully usable:
    * 1. Adds the project to the cache
    * 2. Starts the asynchronous process to compute the digest of files with empty Digest
    * 3. Starts the project view coordinator, that will trigger indexing for all the views in that project
    * 4. Creates the default resources: ElasticSearchView, SparqView, InProjectResolver and DiskStorage
    *
    * @param project the targeted project
    * @param subject the caller who created the project
    */
  def apply(project: Project, subject: Subject): F[Unit] = {
    implicit val caller: Caller = Caller(subject, Set(subject))
    implicit val p              = project
    for {
      _ <- cache.project.replace(project)
      _ <- viewCoordinator.start(project)
      _ <- fileAttributesCoordinator.start(project)
      _ <- List(createResolver, createDiskStorage, createElasticSearchView, createSparqlView).sequence
    } yield ()
  }

  private def asJson(view: View): F[Json] =
    view.as[Json](viewCtx.appendContextOf(resourceCtx)) match {
      case Left(err) =>
        logger.error(s"Could not convert view with id '${view.id}' from Graph back to json. Reason: '${err.message}'")
        F.raiseError(InternalError("Could not decode default view from graph to Json"))
      case Right(json) =>
        F.pure(json.removeKeys(revK, deprecatedK).replaceContext(viewCtxUri).addContext(resourceCtxUri))
    }

  private def asJson(storage: Storage): F[Json] =
    storage.as[Json](storageCtx.appendContextOf(resourceCtx)) match {
      case Left(err) =>
        logger.error(s"Could not convert storage '${storage.id}' from Graph to json. Reason: '${err.message}'")
        F.raiseError(InternalError("Could not decode default storage from graph to Json"))
      case Right(json) =>
        F.pure(json.removeKeys(revK, deprecatedK, algorithmK).replaceContext(storageCtxUri).addContext(resourceCtxUri))
    }

  private def asJson(resolver: Resolver): F[Json] =
    resolver.as[Json](resolverCtx.appendContextOf(resourceCtx)) match {
      case Left(err) =>
        logger.error(s"Could not convert resolver '${resolver.id}' from Graph to json. Reason: '${err.message}'")
        F.raiseError(InternalError("Could not decode default in project resolver from graph to Json"))
      case Right(json) =>
        F.pure(json.removeKeys(revK, deprecatedK, algorithmK).replaceContext(resolverCtxUri).addContext(resourceCtxUri))
    }

  private def createElasticSearchView(implicit project: Project, c: Caller): F[Either[Rejection, Resource]] = {
    implicit val acls: AccessControlLists = AccessControlLists.empty
    val view: View                        = ElasticSearchView.default(project.ref)
    asJson(view).flatMap { json =>
      withRetry(views.create(Id(project.ref, view.id), json, extractUuid = true).value, "ElasticSearchView")
    }
  }

  private def createSparqlView(implicit project: Project, c: Caller): F[Either[Rejection, Resource]] = {
    implicit val acls: AccessControlLists = AccessControlLists.empty
    val view: View                        = SparqlView.default(project.ref)
    asJson(view).flatMap { json =>
      withRetry(views.create(Id(project.ref, view.id), json, extractUuid = true).value, "SparqlView")
    }
  }

  private def createResolver(implicit project: Project, c: Caller): F[Either[Rejection, Resource]] = {
    val resolver: Resolver = InProjectResolver.default(project.ref)
    asJson(resolver).flatMap { json =>
      withRetry(resolvers.create(Id(project.ref, resolver.id), json).value, "InProject")
    }
  }

  private def createDiskStorage(implicit project: Project, s: Subject): F[Either[Rejection, Resource]] = {
    val storage: Storage = DiskStorage.default(project.ref)
    asJson(DiskStorage.default(project.ref)).flatMap { json =>
      withRetry(storages.create(Id(project.ref, storage.id), json).value, "DiskStorage")
    }
  }

  private def withRetry(created: F[Either[Rejection, Resource]], resourceType: String)(
      implicit project: Project
  ): F[Either[Rejection, Resource]] = {
    val internalError: KgError = InternalError(s"Couldn't create default $resourceType for project '${project.ref}'")
    created.mapRetry(createdOrExists, internalError)
  }

}
