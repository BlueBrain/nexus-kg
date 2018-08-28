package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.regex.Pattern.quote

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerSettings
import ch.epfl.bluebrain.nexus.admin.client.types.KafkaEvent._
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, KafkaEvent, Project}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.IllegalEventType
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator.Msg
import ch.epfl.bluebrain.nexus.kg.async.{DistributedCache, ProjectViewCoordinator}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.InProjectResolver
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.service.kafka.KafkaConsumer
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future

// $COVERAGE-OFF$
private class Indexing(resources: Resources[Task], cache: DistributedCache[Task], coordinator: ActorRef)(
    implicit as: ActorSystem,
    config: AppConfig) {

  private val consumerSettings = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)

  private val elasticUUID = "684bd815-9273-46f4-ac1c-0383d4a98254"
  private val sparqlUUID  = "d88b71d2-b8a4-4744-bf22-2d99ef5bd26b"

  private val defaultEsMapping =
    jsonContentOf("/elastic/mapping.json", Map(quote("{{docType}}") -> config.elastic.docType))

  def startAccountStream(): Unit = {

    def index(event: KafkaEvent): Future[Unit] = {
      val update = event match {
        case OrganizationCreated(_, label, uuid, rev, _, org) =>
          cache.addAccount(AccountRef(uuid), Account(org.name, rev, label, deprecated = false, uuid), updateRev = false)
        case OrganizationUpdated(_, label, uuid, rev, _, org) =>
          cache.addAccount(AccountRef(uuid), Account(org.name, rev, label, deprecated = false, uuid), updateRev = true)
        case OrganizationDeprecated(_, uuid, rev, _) =>
          cache.deprecateAccount(AccountRef(uuid), rev)
        case _: ProjectCreated    => throw IllegalEventType("ProjectCreated", "Organization")
        case _: ProjectUpdated    => throw IllegalEventType("ProjectUpdated", "Organization")
        case _: ProjectDeprecated => throw IllegalEventType("ProjectDeprecated", "Organization")
      }
      update.flatMap { updated =>
        if (updated) Task.unit
        else Task.raiseError(new RetriableErr(s"Failed to update account '${event.id}'"))
      }.runAsync
    }

    KafkaConsumer.start(consumerSettings, index, config.kafka.accountTopic, "account-events", committable = false, None)
    ()
  }

  def startProjectStream(): Unit = {

    def processResult(accountRef: AccountRef, projectRef: ProjectRef): Boolean => Task[Unit] = {
      case true =>
        coordinator ! Msg(accountRef, projectRef)
        Task.unit
      case false =>
        Task.raiseError(new RetriableErr(s"Failed to update project '${projectRef.id}'"))
    }

    def index(event: KafkaEvent): Future[Unit] = {
      val update = event match {
        case ProjectCreated(_, label, uuid, orgUUid, rev, _, proj) =>
          cache
            .addProject(
              ProjectRef(uuid),
              AccountRef(orgUUid),
              Project(proj.name, label, proj.prefixMappings, proj.base, rev, deprecated = false, uuid),
              updateRev = false
            )
            .flatMap {
              case true =>
                cache.addResolver(ProjectRef(uuid),
                                  InProjectResolver(ProjectRef(uuid), nxv.InProject.value, 1L, deprecated = false, 1))
              case false => Task(false)
            }
            .flatMap {
              case true =>
                for {
                  elastic <- cache.addView(
                    ProjectRef(uuid),
                    ElasticView(
                      defaultEsMapping,
                      Set.empty,
                      None,
                      includeMetadata = true,
                      sourceAsText = true,
                      ProjectRef(uuid),
                      nxv.defaultElasticIndex.value,
                      elasticUUID,
                      1L,
                      deprecated = false
                    ),
                    true
                  )
                  sparql <- cache.addView(
                    ProjectRef(uuid),
                    SparqlView(
                      ProjectRef(uuid),
                      nxv.defaultSparqlIndex.value,
                      sparqlUUID,
                      1L,
                      deprecated = false
                    ),
                    true
                  )
                } yield elastic && sparql
              case false => Task(false)
            }
            .flatMap(processResult(AccountRef(orgUUid), ProjectRef(uuid)))
        case ProjectUpdated(_, label, uuid, orgUUid, rev, _, proj) =>
          cache
            .addProject(
              ProjectRef(uuid),
              AccountRef(orgUUid),
              Project(proj.name, label, proj.prefixMappings, proj.base, rev, deprecated = false, uuid),
              updateRev = true
            )
            .flatMap(processResult(AccountRef(orgUUid), ProjectRef(uuid)))
        case ProjectDeprecated(_, uuid, orgUUid, rev, _) =>
          cache
            .deprecateProject(ProjectRef(uuid), AccountRef(orgUUid), rev)
            .flatMap(processResult(AccountRef(orgUUid), ProjectRef(uuid)))
        case _: OrganizationCreated    => throw IllegalEventType("OrganizationCreated", "Project")
        case _: OrganizationUpdated    => throw IllegalEventType("OrganizationUpdated", "Project")
        case _: OrganizationDeprecated => throw IllegalEventType("OrganizationDeprecated", "Project")
      }
      update.runAsync
    }

    KafkaConsumer.start(consumerSettings, index, config.kafka.projectTopic, "project-events", committable = false, None)
    ()
  }

  def startResolverStream(): Unit = {
    ResolverIndexer.start(resources, cache)
    ()
  }

  def startViewStream(): Unit = {
    ViewIndexer.start(resources, cache)
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
    * @param cache the distributed cache
    */
  def start(resources: Resources[Task], cache: DistributedCache[Task])(implicit as: ActorSystem,
                                                                       ucl: HttpClient[Task, ResultSet],
                                                                       config: AppConfig): Unit = {

    def selector(view: View, labeledProject: LabeledProject): ActorRef = view match {
      case v: ElasticView => ElasticIndexer.start(v, resources, labeledProject)
      case v: SparqlView  => SparqlIndexer.start(v, resources, labeledProject)
    }

    val coordinator = ProjectViewCoordinator.start(cache, selector, None, config.cluster.shards)
    val indexing    = new Indexing(resources, cache, coordinator)
    indexing.startAccountStream()
    indexing.startProjectStream()
    indexing.startResolverStream()
    indexing.startViewStream()
  }

}
// $COVERAGE-ON$
