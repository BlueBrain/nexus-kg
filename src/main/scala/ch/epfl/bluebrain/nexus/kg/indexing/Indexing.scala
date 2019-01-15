package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

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
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.kg.async.{CacheAggregator, ProjectViewsLifeCycle}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectRef, Resources}
import ch.epfl.bluebrain.nexus.service.kafka.KafkaConsumer
import com.github.ghik.silencer.silent
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.collection._
import scala.concurrent.Future

// $COVERAGE-OFF$
@silent
private class Indexing(resources: Resources[Task], cache: CacheAggregator[Task])(implicit as: ActorSystem,
                                                                                 ucl: HttpClient[Task, ResultSet],
                                                                                 esClient: ElasticClient[Task],
                                                                                 config: AppConfig) {
// TODO: uncomment when KafkaEvents are present in the AdminClient
  private val consumerSettings   = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)
  private implicit val viewCache = cache.view

//  private val elasticUUID = UUID.fromString("684bd815-9273-46f4-ac1c-0383d4a98254")
//  private val sparqlUUID  = UUID.fromString("d88b71d2-b8a4-4744-bf22-2d99ef5bd26b")

  private val orgsPrefix     = config.http.baseIri + "orgs"
  private val projectsPrefix = config.http.baseIri + "projects"

  private val projViewMap: concurrent.Map[UUID, ProjectViewsLifeCycle] =
    new ConcurrentHashMap[UUID, ProjectViewsLifeCycle]().asScala

//  private val defaultEsMapping =
//    jsonContentOf("/elastic/mapping.json", Map(Pattern.quote("{{docType}}") -> config.elastic.docType))

  def startKafkaStream(): Unit = {

// TODO:
//    def defaultEsView(projectRef: ProjectRef): ElasticView =
//      ElasticView(defaultEsMapping,
//                  Set.empty,
//                  None,
//                  includeMetadata = true,
//                  sourceAsText = true,
//                  projectRef,
//                  nxv.defaultElasticIndex.value,
//                  elasticUUID,
//                  1L,
//                  deprecated = false)
//
//    def defaultSparqlView(projectRef: ProjectRef): SparqlView =
//      SparqlView(projectRef, nxv.defaultSparqlIndex.value, sparqlUUID, 1L, deprecated = false)
//
//    def defaultInProjectResolver(projectRef: ProjectRef): InProjectResolver =
//      InProjectResolver(projectRef, nxv.InProject.value, 1L, deprecated = false, 1)

    implicit val icc: IamClientConfig = config.iam.iamClient

    def updateProjectOnViewsLifeCycle(project: Project): Task[Option[ProjectViewsLifeCycle]] =
      projViewMap
        .get(project.uuid)
        .map(_.updateProject(project))
        .getOrElse(Task.pure(ProjectViewsLifeCycle(resources, project)))
        .map(projViewMap.put(project.uuid, _))

    def index(event: Event): Future[Unit] = {

      val update = event match {
        case OrganizationDeprecated(uuid, _, _, _) =>
          cache.project.list(OrganizationRef(uuid)).map(_.map(proj => projViewMap.get(proj.uuid)).flatten).flatMap {
            viewLifeCycle =>
              Task.sequence(viewLifeCycle.map(_.deprecateViews))
          } *> Task.unit
        case ProjectCreated(uuid, label, orgUuid, orgLabel, desc, am, base, vocab, instant, subject) =>
          // format: off
          val project = Project(projectsPrefix + label, label, orgLabel, desc, base, vocab, am, uuid, orgUuid, 1L, deprecated = false, instant, subject.id, instant, subject.id)
          // format: on
          cache.project.replace(project).flatMap(_ => updateProjectOnViewsLifeCycle(project)) *> Task.unit
        case ProjectUpdated(uuid, label, desc, am, base, vocab, rev, instant, subject) =>
          cache.project.get(ProjectRef(uuid)).flatMap {
            case Some(project) =>
              // format: off
              val newProject = Project(projectsPrefix + label, label, project.organizationLabel, desc, base, vocab, am, uuid, project.organizationUuid, rev, deprecated = false, instant, subject.id, instant, subject.id)
              // format: on
              cache.project.replace(newProject).flatMap(_ => updateProjectOnViewsLifeCycle(newProject)) *> Task.unit
            case None => Task.unit
          }
        case ProjectDeprecated(uuid, rev, _, _) =>
          cache.project.deprecate(ProjectRef(uuid), rev)
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
  def start(resources: Resources[Task], cache: CacheAggregator[Task])(implicit as: ActorSystem,
                                                                      ucl: HttpClient[Task, ResultSet],
                                                                      config: AppConfig): Unit = {
    implicit val mt            = ActorMaterializer()
    implicit val ul            = untyped[Task]
    implicit val elasticClient = ElasticClient[Task](config.elastic.base)

    val indexing = new Indexing(resources, cache)
//    indexing.startKafkaStream()
    indexing.startResolverStream()
    indexing.startViewStream()
  }

}
// $COVERAGE-ON$
