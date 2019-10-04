package ch.epfl.bluebrain.nexus.kg.routes

import akka.Done
import akka.actor.ActorSystem
import akka.cluster.{Cluster, MemberStatus}
import akka.event.Logging
import akka.persistence.cassandra.CassandraPluginConfig
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.PersistenceConfig
import monix.eval.Task

import scala.concurrent.Future

sealed trait Status {

  /**
    * Checks the connectivity.
    *
    * @return Future(true) when there is connectivity with the service from within the app
    *         Future(false) otherwise
    */
  def check: Task[Boolean]
}

object Status {

  class CassandraStatus(implicit as: ActorSystem, persistence: PersistenceConfig) extends Status {
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

  class ClusterStatus(cluster: Cluster) extends Status {
    override def check: Task[Boolean] =
      Task.pure(
        !cluster.isTerminated &&
          cluster.state.leader.isDefined && cluster.state.members.nonEmpty &&
          !cluster.state.members.exists(_.status != MemberStatus.Up) && cluster.state.unreachable.isEmpty
      )
  }
}
