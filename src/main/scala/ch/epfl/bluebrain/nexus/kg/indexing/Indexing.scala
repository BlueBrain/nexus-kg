package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.ActorMaterializer
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event._
import ch.epfl.bluebrain.nexus.admin.client.types.events.decoders._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.untyped
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.async.{Caches, ProjectViewCoordinator, ProjectViewCoordinatorActor}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.InProjectResolver
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.encoding._
import ch.epfl.bluebrain.nexus.service.kafka.KafkaConsumer
import com.github.ghik.silencer.silent
import io.circe.Json
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future

// $COVERAGE-OFF$
@silent
private class Indexing(resources: Resources[Task], cache: Caches[Task], coordinator: ProjectViewCoordinator[Task])(
    implicit as: ActorSystem,
    config: AppConfig) {

  private val logger                                          = Logger[this.type]
  private val consumerSettings                                = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)
  private implicit val validation: AdditionalValidation[Task] = AdditionalValidation.pass

  private def asJson(view: View): Json =
    view
      .asJson(viewCtx.appendContextOf(resourceCtx))
      .removeKeys("@context", "_rev", "_deprecated")
      .addContext(viewCtxUri)
      .addContext(resourceCtxUri)

  private def asJson(resolver: Resolver): Json =
    resolver
      .asJson(resolverCtx.appendContextOf(resourceCtx))
      .removeKeys("@context", "_rev", "_deprecated")
      .addContext(resolverCtxUri)
      .addContext(resourceCtxUri)

  def startKafkaStream(): Unit = {

    def index(event: Event): Future[Unit] = {
      logger.debug(s"Handling admin event: '$event'")
      val update = event match {
        case OrganizationDeprecated(uuid, _, _, _) =>
          coordinator.stop(OrganizationRef(uuid))

        case ProjectCreated(uuid, label, orgUuid, orgLabel, desc, am, base, vocab, instant, subject) =>
          // format: off
          val project = Project(config.http.projectsIri + label, label, orgLabel, desc, base, vocab, am, uuid, orgUuid, 1L, deprecated = false, instant, subject.id, instant, subject.id)
          // format: on
          implicit val s         = subject
          val elasticView: View  = ElasticView.default(project.ref)
          val sparqlView: View   = SparqlView.default(project.ref)
          val resolver: Resolver = InProjectResolver.default(project.ref)
          cache.project.replace(project) *>
            resources.create(Id(project.ref, elasticView.id), viewRef, asJson(elasticView)).value *>
            resources.create(Id(project.ref, sparqlView.id), viewRef, asJson(sparqlView)).value *>
            resources.create(Id(project.ref, resolver.id), resolverRef, asJson(resolver)).value *>
            coordinator.start(project)

        case ProjectUpdated(uuid, label, desc, am, base, vocab, rev, instant, subject) =>
          cache.project.get(ProjectRef(uuid)).flatMap {
            case Some(project) =>
              // format: off
              val newProject = Project(config.http.projectsIri + label, label, project.organizationLabel, desc, base, vocab, am, uuid, project.organizationUuid, rev, deprecated = false, instant, subject.id, instant, subject.id)
              // format: on
              cache.project.replace(newProject) *> coordinator.change(newProject, project)
            case None => Task.unit
          }
        case ProjectDeprecated(uuid, rev, _, _) =>
          cache.project.deprecate(ProjectRef(uuid), rev) *> coordinator.stop(ProjectRef(uuid))
      }
      update.runToFuture
    }

    KafkaConsumer.start(consumerSettings, index, config.kafka.adminTopic, "admin-events", committable = false, None)
    ()
  }

  def startResolverStream(): Unit = {
    ResolverIndexer.start(resources, cache.resolver)
    ()
  }

  def startViewStream(): Unit = {
    ViewIndexer.start(resources, cache.view)
    ()
  }
}

object Indexing {

  /**
    * Starts all indexing streams:
    * <ul>
    *   <li>Views</li>
    *   <li>Projects</li>
    *   <li>Accounts</li>
    *   <li>Resolvers</li>
    * </ul>
    *
    * @param resources the resources operations
    * @param cache     the distributed cache
    */
  def start(resources: Resources[Task], cache: Caches[Task])(implicit as: ActorSystem,
                                                             ucl: HttpClient[Task, ResultSet],
                                                             config: AppConfig): Unit = {
    implicit val mt            = ActorMaterializer()
    implicit val ul            = untyped[Task]
    implicit val elasticClient = ElasticClient[Task](config.elastic.base)

    val coordinatorRef = ProjectViewCoordinatorActor.start(resources, cache.view, None, config.cluster.shards)
    val coordinator    = new ProjectViewCoordinator[Task](cache, coordinatorRef)

    val indexing = new Indexing(resources, cache, coordinator)
//    indexing.startKafkaStream()
    indexing.startResolverStream()
    indexing.startViewStream()
  }

}
// $COVERAGE-ON$
