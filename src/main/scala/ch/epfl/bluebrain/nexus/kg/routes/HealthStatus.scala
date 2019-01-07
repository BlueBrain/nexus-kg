package ch.epfl.bluebrain.nexus.kg.routes

import akka.Done
import akka.actor.ActorSystem
import akka.cluster.{Cluster, MemberStatus}
import akka.event.Logging
import akka.persistence.cassandra.CassandraPluginConfig
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.PersistenceConfig
import journal.Logger
import monix.eval.Task
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._

import scala.concurrent.Future

sealed trait HealthStatus {

  /**
    * Checks the connectivity.
    *
    * @return Future(true) when there is connectivity with the service from within the app
    *         Future(false) otherwise
    */
  def check: Task[Boolean]
}

object HealthStatus {

  class CassandraHealthStatus(implicit as: ActorSystem, persistence: PersistenceConfig) extends HealthStatus {
    implicit val ec     = as.dispatcher
    private val log     = Logging(as, "CassandraHeathCheck")
    private val config  = new CassandraPluginConfig(as, as.settings.config.getConfig(persistence.journalPlugin))
    private val (p, s)  = (config.sessionProvider, config.sessionSettings)
    private val session = new CassandraSession(as, p, s, ec, log, "health", _ => Future.successful(Done.done()))
    private val query   = s"SELECT now() FROM ${config.keyspace}.messages;"

    override def check: Task[Boolean] =
      Task.deferFuture(session.selectOne(query).map(_ => true).recover {
        case err =>
          log.error("Error while attempting to query for health check", err)
          false
      })
  }

  class ClusterHealthStatus(cluster: Cluster) extends HealthStatus {
    override def check: Task[Boolean] =
      Task.pure(
        !cluster.isTerminated &&
          cluster.state.leader.isDefined && cluster.state.members.nonEmpty &&
          !cluster.state.members.exists(_.status != MemberStatus.Up) && cluster.state.unreachable.isEmpty)
  }

  class IamHealthStatus(client: IamClient[Task]) extends HealthStatus {
    private implicit val log                      = Logger(s"${getClass.getSimpleName}")
    private implicit val token: Option[AuthToken] = None

    override def check: Task[Boolean] = client.acls(/).map(_ => true).transformAndCatchError("fetch ACLs")
  }

  class AdminHealthStatus(client: AdminClient[Task]) extends HealthStatus {
    private implicit val log                      = Logger(s"${getClass.getSimpleName}")
    private implicit val token: Option[AuthToken] = None
    override def check: Task[Boolean]             = client.getAccount("test").transformAndCatchError("fetch account")
  }

  class ElasticSearchHealthStatus(client: ElasticClient[Task]) extends HealthStatus {
    private implicit val log          = Logger(s"${getClass.getSimpleName}")
    override def check: Task[Boolean] = client.existsIndex("test").transformAndCatchError("index exists")
  }

  class SparqlHealthStatus(client: BlazegraphClient[Task]) extends HealthStatus {
    private implicit val log          = Logger(s"${getClass.getSimpleName}")
    override def check: Task[Boolean] = client.namespaceExists.transformAndCatchError("namespace exists")
  }

  private implicit class TaskSyntax[A](private val task: Task[A]) extends AnyVal {
    def transformAndCatchError(message: String)(implicit log: Logger): Task[Boolean] =
      task.map(_ => true).onErrorRecover {
        case err =>
          log.error(s"Error while attempting to query '$message' for health check", err)
          false
      }
  }
}
