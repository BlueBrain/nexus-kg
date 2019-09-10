#!/usr/bin/env amm

import coursier.core.Authentication, coursier.MavenRepository

interp.repositories() ++= Seq(MavenRepository("http://dl.bintray.com/bbp/nexus-releases"))
interp.repositories() ++= Seq(MavenRepository("http://dl.bintray.com/bbp/nexus-snapshots"))

@ import $ivy.{
  `ch.epfl.bluebrain.nexus::admin-client:64496380`,
  `ch.epfl.bluebrain.nexus::elasticsearch-client:0.17.8`,
  `ch.epfl.bluebrain.nexus::sparql-client:0.17.7`,
  `ch.qos.logback:logback-classic:1.2.3`,
  `com.github.alexarchambault::case-app:2.0.0-M9`,
  `com.lightbend.akka::akka-stream-alpakka-cassandra:1.1.1`,
  `com.typesafe.akka::akka-http:10.1.9`,
  `io.monix::monix-eval:3.0.0-RC3`,
  `io.verizon.journal::core:3.0.19`,
  `org.scala-lang.modules::scala-xml:1.2.0`
}

import java.net.InetSocketAddress
import java.nio.file._
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.unmarshalling.PredefinedFromEntityUnmarshallers
import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.alpakka.cassandra.scaladsl.CassandraSource
import akka.stream.Materializer
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.{Flow, Framing, Sink, Source}
import akka.util.ByteString
import caseapp._
import caseapp.core.argparser._
import caseapp.core.Error._
import cats.effect.{Effect, IO, LiftIO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.config.AdminClientConfig
import ch.epfl.bluebrain.nexus.admin.client.types.Organization
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlResults}
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import com.datastax.driver.core._
import com.datastax.driver.core.policies.AddressTranslator
import com.datastax.driver.core.{Cluster, Session, SimpleStatement}
import delete.blazegraph.client.BlazegraphNamespaceClient
import delete.blazegraph.client.BlazegraphNamespaceClientError._
import delete.config._
import delete.config.AppConfig._
import monix.eval.Task
import monix.execution.Scheduler
import os._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.xml.XML

object delete {

  object blazegraph {

    object client {

      class BlazegraphNamespaceClient[F[_]](client: HttpClient[F, String])(implicit config: AppConfig, F: Effect[F]) {

        def namespaces(): F[Seq[String]] =
          client(HttpRequest(uri = s"${config.blazegraphBase}/namespace")).flatMap { string =>
            Try(XML.loadString(string)) match {
              case Success(xml) => F.pure((xml \\ "title").map(_.text))
              case Failure(_)   => F.raiseError(NotXmlResponse(string))
            }
          }
      }

      @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
      sealed abstract class BlazegraphNamespaceClientError(val msg: String)
          extends Exception
          with Product
          with Serializable {
        override def fillInStackTrace(): BlazegraphNamespaceClientError = this
        override def getMessage: String                                 = msg
      }
      object BlazegraphNamespaceClientError {
        final case class NotXmlResponse(string: String)
            extends BlazegraphNamespaceClientError(s"Expected response format XML, found '$string'")
      }

      object BlazegraphNamespaceClient extends PredefinedFromEntityUnmarshallers {
        def apply[F[_]](
            implicit config: AppConfig,
            F: Effect[F],
            ec: ExecutionContext,
            mt: Materializer,
            cl: UntypedHttpClient[F]
        ): BlazegraphNamespaceClient[F] = {
          implicit val client: HttpClient[F, String] = HttpClient.withUnmarshaller[F, String]
          new BlazegraphNamespaceClient(client)
        }
      }
    }
  }

  object config {

    final case class AppConfig(
        admin: AdminClientConfig,
        maxBatchDelete: Int = 20,
        maxSelect: Int = 20,
        adminKeyspace: String,
        kgKeyspace: String,
        saToken: Option[AuthToken],
        blazegraphBase: Uri,
        elasticsearchBase: Uri,
        elasticSearchPrefix: String,
        cassandra: CassandraConfig
    )

    object AppConfig {
      final case class CassandraConfig(credentials: Option[CassandraCredentials], contactPoints: CassandraContactPoints)
      final case class CassandraCredentials(username: String, password: String)
      implicit def saTokenOpt(implicit config: AppConfig): Option[AuthToken] = config.saToken
    }

    @AppName("Nexus Delete Projects")
    @AppVersion("0.1.0")
    @ProgName("delete-projects.sc")
    final case class Args(
        @ExtraName("cp")
        @HelpMessage("A cassandra contact point, in the following format: host:port:address")
        contactPoints: List[CassandraContactPoint],
        @ExtraName("u")
        @HelpMessage("Cassandra username (if required)")
        username: Option[String] = None,
        @ExtraName("p")
        @HelpMessage("Cassandra password (if required)")
        password: Option[String] = None,
        @HelpMessage("kg cassandra keyspace")
        kgKeyspace: Option[String],
        @HelpMessage("admin cassandra keyspace")
        adminKeyspace: Option[String],
        @HelpMessage("Maximum number of batch DELETE operations on cassandra")
        maxBatchDelete: Option[Int],
        @HelpMessage("Maximum number of rows returned from a single SELECT query")
        maxSelect: Option[Int],
        @ExtraName("es")
        @HelpMessage("ElasticSearch endpoint")
        elasticSearch: AbsoluteIri,
        @ExtraName("es-prefix")
        @HelpMessage("ElasticSearch indexes prefix")
        elasticSearchPrefix: Option[String],
        @HelpMessage("Blazegraph endpoint")
        blazegraph: AbsoluteIri,
        @HelpMessage("Admin service endpoint")
        admin: AbsoluteIri,
        @HelpMessage("Nexus Token. Used to call admin service")
        token: Option[String] = None,
        @HelpMessage("Target project(s) in the following format: {org}/{project}")
        project: List[ProjectLabel] = List.empty,
        @HelpMessage("Target organization(s)")
        org: List[String] = List.empty
    ) {
      private def credentials: Option[CassandraCredentials] = (username -> password) match {
        case (Some(u), Some(p)) => Some(CassandraCredentials(u, p))
        case _                  => None
      }
      def toAppConfig: AppConfig =
        AppConfig(
          AdminClientConfig(admin, admin),
          maxBatchDelete.getOrElse(20),
          maxSelect.getOrElse(20),
          adminKeyspace.getOrElse("admin"),
          kgKeyspace.getOrElse("kg"),
          token.map(AuthToken(_)),
          blazegraph.toAkkaUri,
          elasticSearch.toAkkaUri,
          elasticSearchPrefix.getOrElse("kg_"),
          CassandraConfig(credentials, CassandraContactPoints(contactPoints))
        )
    }

    final case class ProjectLabel(org: String, project: String) {
      override def toString: String = s"$org/$project"
    }

    implicit val absoluteIriParser: ArgParser[AbsoluteIri] = SimpleArgParser.from[AbsoluteIri]("absolute iri") { s =>
      Iri.absolute(s).left.map(_ => Other(s"Invalid format. Expected an Absolute iri. Found: '$s'"))
    }

    implicit val projectLabelParser: ArgParser[ProjectLabel] = SimpleArgParser.from[ProjectLabel]("project label") {
      s =>
        Try(s.split("/", 2)) match {
          case Success(Array(org, project)) if !project.contains("/") =>
            Right(ProjectLabel(org, project))
          case _ => Left(Other(s"Invalid format. Expected: '{org}/{project}'. Found: '$s'"))
        }
    }

    implicit val contactPointParser: ArgParser[CassandraContactPoint] = {
      def isInt(s: String) = s.nonEmpty && s.forall(_.isDigit)
      SimpleArgParser.from[CassandraContactPoint]("cp") { s =>
        Try(s.split(":", 3)) match {
          case Success(Array(host, port, addr)) if !addr.contains(":") && isInt(port) =>
            Right(CassandraContactPoint(host, port.toInt, addr))
          case _ => Left(Other(s"Invalid format. Expected: '{host:port:address}'. Found: '$s'"))
        }
      }
    }

    final case class CassandraContactPoints(contactPoints: Seq[CassandraContactPoint]) {
      def toAddress: Seq[InetSocketAddress] = contactPoints.map(_.toAddress)
      def toAddressTranslator: AddressTranslator = new AddressTranslator {

        override def init(cluster: Cluster) = {}

        override def translate(address: InetSocketAddress): InetSocketAddress =
          contactPoints.find(c => s"/${c.address}" == address.getAddress.toString) match {
            case Some(CassandraContactPoint(host, port, _)) => new InetSocketAddress(host, port)
            case _ =>
              throw new IllegalArgumentException(s"Not found mapping for address '${address.getAddress.toString}'")
          }

        override def close() = {}
      }
    }

    final case class CassandraContactPoint(host: String, port: Int, address: String) {
      def toAddress: InetSocketAddress = Try(new InetSocketAddress(host, port)) match {
        case Failure(_) =>
          throw new IllegalArgumentException(s"Unable to convert host '$host' and port '$port' to address")
        case Success(addr) => addr
      }
    }
  }
}

type TagViewsPk = (String, Long, UUID, Long, String)
type MessagesPk = (String, Long)

implicit class RichFuture[A](private val future: Future[A]) extends AnyVal {
  def to[F[_]](implicit F: LiftIO[F]): F[A] =
    F.liftIO(IO.fromFuture(IO(future)))
}
def selectStmt(stmt: String)(implicit session: Session, appConfig: AppConfig): Source[Row, NotUsed] =
    CassandraSource(new SimpleStatement(stmt).setFetchSize(appConfig.maxSelect))

val flowMessages: Flow[Row, (String, Long), NotUsed] = Flow[Row].map { row =>
    val persistenceId = row.getString("persistence_id")
    val partitionNr   = row.getLong("partition_nr")
    (persistenceId, partitionNr)
  }

val flowTagViews: Flow[Row, TagViewsPk, NotUsed] = Flow[Row].map { row =>
    val tagName         = row.getString("tag_name")
    val timeBucket      = row.getLong("timebucket")
    val timestamp       = row.getUUID("timestamp")
    val tagPidSeqNumber = row.getLong("tag_pid_sequence_nr")
    val persistenceId   = row.getString("persistence_id")
    (tagName, timeBucket, timestamp, tagPidSeqNumber, persistenceId)
  }

def flowBatchDelete(implicit session: Session, config: AppConfig): Flow[SimpleStatement, Boolean, NotUsed] =
    Flow[SimpleStatement]
      .groupedWithin(config.maxBatchDelete, 1 second)
      .map { seq =>
        val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
        seq.foreach(batch.add)
        session.execute(batch).wasApplied()
      }

def selectFromMsg(
      keyspace: String,
      prefix: String
  )(implicit session: Session, appConfig: AppConfig): Source[MessagesPk, NotUsed] = {
    val stmt = s"SELECT persistence_id,partition_nr FROM $keyspace.messages"
    selectStmt(stmt).via(flowMessages).filter { case (persistenceId, _) => persistenceId.startsWith(prefix) }
  }

def selectFromTagViews(
      keyspace: String,
      prefix: String
  )(implicit session: Session, appConfig: AppConfig): Source[TagViewsPk, NotUsed] = {
    val stmt = s"SELECT tag_name,timebucket,timestamp,tag_pid_sequence_nr,persistence_id FROM $keyspace.tag_views"
    selectStmt(stmt).via(flowTagViews).filter { case (_, _, _, _, persistenceId) => persistenceId.startsWith(prefix) }
  }

def deleteStmt[F[_]: Effect](
      keyspace: String,
      source: Source[Either[MessagesPk, TagViewsPk], NotUsed]
  )(implicit session: Session, config: AppConfig, mt: ActorMaterializer, ec: ExecutionContext): F[Unit] = {
    val path = Files.createTempFile(keyspace, "stmt")
    val sourceStmt = source
      .mapConcat {
        case Left((persistenceId, partitionNr)) =>
          List(
            s"DELETE FROM $keyspace.messages WHERE persistence_id='$persistenceId' AND partition_nr=$partitionNr",
            s"DELETE FROM $keyspace.tag_scanning WHERE persistence_id='$persistenceId'",
            s"DELETE FROM $keyspace.tag_write_progress WHERE persistence_id='$persistenceId'"
          )
        case Right((tagName, timeBucket, timestamp, tagPidSeqNumber, persistenceId)) =>
          List(
            s"DELETE FROM $keyspace.tag_views WHERE persistence_id='$persistenceId' AND tag_name='$tagName' AND timebucket=$timeBucket AND timestamp=$timestamp AND tag_pid_sequence_nr=$tagPidSeqNumber"
          )
      }
    (sourceStmt.map(s => ByteString(s + "\n")).runWith(FileIO.toPath(path)) >>
      FileIO
        .fromPath(path)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
        .map(line => new SimpleStatement(line.utf8String))
        .via(flowBatchDelete)
        .runWith(Sink.ignore) >> Future(Files.delete(path))).to[F]
  }

def deleteRows[F[_]](keyspace: String, prefix: String)(
      implicit session: Session,
      config: AppConfig,
      mt: ActorMaterializer,
      ec: ExecutionContext,
      F: Effect[F]
  ): F[Unit] = {
    val sourceMessages = selectFromMsg(keyspace, prefix).map[Either[MessagesPk, TagViewsPk]](Left(_))
    val sourceTagViews = selectFromTagViews(keyspace, prefix).map[Either[MessagesPk, TagViewsPk]](Right(_))
    deleteStmt(keyspace, sourceMessages.merge(sourceTagViews))
  }

def deleteRows[F[_]](project: Project)(
      implicit session: Session,
      config: AppConfig,
      mt: ActorMaterializer,
      ec: ExecutionContext,
      F: Effect[F]
  ): F[Unit] =
    deleteRows(config.kgKeyspace, s"resources-${project.uuid}") >>
      deleteRows(config.adminKeyspace, s"projects-${project.uuid}")

def deleteRows[F[_]](org: Organization)(
      implicit session: Session,
      config: AppConfig,
      mt: ActorMaterializer,
      ec: ExecutionContext,
      F: Effect[F]
  ): F[Unit] =
    deleteRows(config.adminKeyspace, s"organizations-${org.uuid}")

def createSession(implicit config: AppConfig): Session = {
    val sessionBuilder = Cluster.builder
      .addContactPointsWithPorts(config.cassandra.contactPoints.toAddress: _*)
      .withAddressTranslator(config.cassandra.contactPoints.toAddressTranslator)

    config.cassandra.credentials match {
      case Some(CassandraCredentials(username, password)) =>
        sessionBuilder.withCredentials(username, password).build.connect()
      case _ =>
        sessionBuilder.build.connect()
    }
  }
def printWarn(value: String) = println(s"${Console.YELLOW}$value${Console.RESET}")

def deleteProjects(projects: List[ProjectLabel], orgs: List[String])(
      implicit config: AppConfig,
      session: Session,
      adminClient: AdminClient[Task],
      as: ActorSystem,
      mt: ActorMaterializer,
      ec: ExecutionContext,
      scheduler: Scheduler
  ): Task[Unit] = {
    implicit val utClient            = untyped[Task]
    implicit val sparqlResultsClient = withUnmarshaller[Task, SparqlResults]

    val namespaceClient     = BlazegraphNamespaceClient[Task]
    val elasticSearchClient = ElasticSearchClient[Task](config.elasticsearchBase)

    def deleteNs(namespace: String): Task[Boolean] =
      BlazegraphClient[Task](config.blazegraphBase, namespace, None).deleteNamespace

    def deleteBlazegraph(project: Project): Task[Seq[(String, Boolean)]] = {
      println(s"Deleting Blazegraph namespaces for project '${project.show}' (uuid '${project.uuid}')")
      namespaceClient
        .namespaces()
        .flatMap(
          seq => Task.sequence(seq.filter(_.contains(project.uuid.toString)).map(ns => deleteNs(ns).map(ns -> _)))
        )
    }

    def deleteElasticSearch(project: Project): Task[Boolean] = {
      println(s"Deleting ElasticSearch indices for project '${project.show}' (uuid '${project.uuid}')")
      elasticSearchClient.deleteIndex(s"${config.elasticSearchPrefix}${project.uuid}*")
    }

    def deleteCassandra(project: Project): Task[Unit] = {
      println(s"Deleting cassandra rows for project '${project.show}' (uuid '${project.uuid}')")
      deleteRows[Task](project)
    }

    def collectAndLog(projects: List[(ProjectLabel, Option[Project])]): List[Project] =
      projects.foldLeft(List.empty[Project]) {
        case (acc, (label, None)) =>
          printWarn(s"Project not found '$label'. Ignoring ...")
          acc
        case (acc, (_, Some(project))) =>
          println(s"Project Found  ${project.show} ...")
          project :: acc
      }

  val resolvedProjects = for {
      fromOrgs <- orgs.map(adminClient.fetchProjects(_).map(_.results.map(_.source))).sequence.map(_.flatten)
      fromProjects <- projects
        .map(p => adminClient.fetchProject(p.org, p.project).map(p -> _))
        .sequence
        .map(collectAndLog)
    } yield fromOrgs ++ fromProjects
    resolvedProjects
      .flatMap { projects =>
        projects
          .map(proj => deleteCassandra(proj) >> deleteBlazegraph(proj) >> deleteElasticSearch(proj))
          .sequence >> Task.unit
      }
  }

def deleteOrgs(orgs: List[String])(
      implicit config: AppConfig,
      session: Session,
      adminClient: AdminClient[Task],
      as: ActorSystem,
      mt: ActorMaterializer,
      ec: ExecutionContext,
      scheduler: Scheduler
  ): Task[Unit] = {
    def deleteCassandra(org: Organization): Task[Unit] = {
      println(s"Deleting cassandra rows for org '${org.label}' (uuid '${org.uuid}')")
      deleteRows[Task](org)
    }

    def collectAndLog(orgs: List[(String, Option[Organization])]): List[Organization] =
      orgs.foldLeft(List.empty[Organization]) {
        case (acc, (org, None)) =>
          printWarn(s"Organization not found '$org'. Ignoring ...")
          acc
        case (acc, (_, Some(org))) => org :: acc
      }

    val resolvedOrgs =
      orgs.map(label => adminClient.fetchOrganization(label).map(label -> _)).sequence.map(collectAndLog)
    resolvedOrgs.flatMap(_.map(deleteCassandra(_)).sequence >> Task.unit)
  }

@main
  def parseArgs(args: String*): Unit = {
    System.setProperty("logback.configurationFile", (os.pwd / "logback.xml").toString)
    if (args.isEmpty || (args.size == 1 && args(0) == "-h")) {
      CaseApp.printHelp[Args]()
    } else {
      CaseApp.parse[Args](args) match {
        case Left(err) =>
          println(err.message + "\n")

        case Right((cfg, _)) if cfg.project.isEmpty && cfg.org.isEmpty =>
          println("At least one project has to be specified")

        case Right((cfg, _)) =>
          implicit val appConfig                      = cfg.toAppConfig
          implicit val system                         = ActorSystem("DeleteProjects")
          implicit val mt                             = ActorMaterializer()
          implicit val scheduler: Scheduler           = Scheduler.global
          implicit val adminClient: AdminClient[Task] = AdminClient[Task](appConfig.admin)
          implicit val session                        = createSession
          (deleteProjects(cfg.project, cfg.org) >> deleteOrgs(cfg.org)).runSyncUnsafe()
          session.close()
      }
    }
  }
