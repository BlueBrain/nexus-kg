import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.{Arrays => JArrays, HashSet => JHashSet, Set => JSet}

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.lightbend.akka:akka-stream-alpakka-cassandra_2.12:1.0.1`
import $ivy.`com.typesafe.scala-logging:scala-logging_2.12:3.9.2`
import $ivy.`io.circe:circe-core_2.12:0.11.1`
import $ivy.`io.circe:circe-parser_2.12:0.11.1`
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.cassandra.scaladsl.{CassandraSink, CassandraSource}
import akka.stream.scaladsl.Source
import ch.qos.logback.classic.Level
import com.datastax.driver.core._
import com.typesafe.scalalogging.Logger
import io.circe.Json
import io.circe.parser.parse
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

val log = Logger("V1ToV11Migration")

val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
root.setLevel(Level.INFO)

val alphaType = "https://bluebrain.github.io/nexus/vocabulary/Alpha"

@main def main(volume: String = "/opt/binaries",
  cassandraContactPoint: String = "127.0.0.1",
  cassandraPort: Int = 9042,
  cassandraUsername: String = "",
  cassandraPassword: String = "",
  adminKeyspace: String = "admin",
  kgKeyspace: String = "kg"): Unit = {

  implicit val system: ActorSystem    = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val session = Cluster.builder
    .addContactPoint(cassandraContactPoint)
    .withPort(cassandraPort)
    .withCredentials(cassandraUsername, cassandraPassword)
    .build
    .connect()

  log.info(s"Dropping table $kgKeyspace.projections")
  session.execute(s"DROP TABLE IF EXISTS $kgKeyspace.projections")
  val uuids = projectUuidOrgUuid(adminKeyspace)
  migrateMessagesTable(kgKeyspace, volume, uuids)
}

private def projectUuidOrgUuid(keyspace: String)(implicit session: Session,
  mat: ActorMaterializer): Map[String, String] = {
  val stmt = new SimpleStatement(
    s"SELECT * FROM $keyspace.messages WHERE event_manifest='project' AND sequence_nr=1 ALLOW FILTERING").setFetchSize(20)
  val done = CassandraSource(stmt)
    .map(
      row =>
        (row.getObject("persistence_id"),
          row.getObject("sequence_nr"),
          StandardCharsets.UTF_8.decode(row.get[ByteBuffer]("event", TypeCodec.blob())).toString))
    .flatMapConcat {
      case (persistenceId, sequenceNr, eventString) =>
        parse(eventString) match {
          case Right(json) =>
            val result = for {
              projUuid <- json.hcursor.get[String]("id")
              orgUuid  <- json.hcursor.get[String]("organizationUuid")
            } yield projUuid -> orgUuid

            result match {
              case Right(uuids) => Source.single(uuids)
              case Left(err) =>
                log.error(s"Unable to find 'id' or 'organizationUuid' on event '$json'", err)
                Source.empty
            }
          case Left(_) =>
            log.error(s"Unable to decode event for persistence_id = '$persistenceId' and sequence_nr = '$sequenceNr'")
            Source.empty
        }
    }
    .runFold(Map.empty[String, String]) { case (acc, (projUUid, orgUuid)) => acc + (projUUid -> orgUuid) }

  Await.result(done, Duration.Inf)
}

private def migrateMessagesTable(keyspace: String, volume: String, uuids: Map[String, String])(implicit session: Session,
  mat: ActorMaterializer) = {

  log.info(s"Starting migration of '$keyspace.messages' table")
  val migratedCount = new AtomicLong()
  val stmt          = new SimpleStatement(s"SELECT * FROM $keyspace.messages").setFetchSize(20)

  val preparedStatement =
    session.prepare(
      s"UPDATE $keyspace.messages SET event = textasblob(?), tags = tags + ?, tags = tags - {'type=$alphaType'} WHERE persistence_id = ? AND  partition_nr = ? AND sequence_nr = ? AND timestamp = ? AND timebucket = ?")
  //#prepared-statement

  //#statement-binder
  val statementBinder =
    (parameters: (Object, Object, Object, Object, Object, String, String), statement: PreparedStatement) =>
      parameters match {
        case (persistenceId, partitionNr, sequenceNr, timestamp, timebucket, event, org) =>
          val orgSet: JSet[String] = new JHashSet[String](JArrays.asList(s"org=$org"))
          statement.bind(event, orgSet, persistenceId, partitionNr, sequenceNr, timestamp, timebucket)
      }

  val sink = CassandraSink[(Object, Object, Object, Object, Object, String, String)](parallelism = 1,
    preparedStatement,
    statementBinder)

  val done = CassandraSource(stmt)
    .map(
      row =>
        (row.getObject("persistence_id"),
          row.getObject("partition_nr"),
          row.getObject("sequence_nr"),
          row.getObject("timestamp"),
          row.getObject("timebucket"),
          StandardCharsets.UTF_8.decode(row.get[ByteBuffer]("event", TypeCodec.blob())).toString))
    .flatMapConcat {
      case (persistenceId, partitionNr, sequenceNr, timestamp, timebucket, eventString) =>
        parse(eventString) match {
          case Right(json) =>
            getOrganization(json, uuids) match {
              case Some(org) =>
                val withOrg = json deepMerge Json.obj("organization" -> Json.fromString(org))
                val withOrgClean = removeAlpha(withOrg)
                val tpe = json.asObject.flatMap(_("@type")).flatMap(_.asString)

                tpe match {
                  case Some("FileCreated") | Some("FileUpdated") =>
                    migratedCount.incrementAndGet()
                    Source.single(
                      (persistenceId,
                        partitionNr,
                        sequenceNr,
                        timestamp,
                        timebucket,
                        transformEvent(withOrgClean, volume).noSpaces, org))
                  case _ =>
                    migratedCount.incrementAndGet()
                    Source.single(
                      (persistenceId, partitionNr, sequenceNr, timestamp, timebucket, withOrgClean.noSpaces, org))
                }
              case _ => Source.empty
            }
          case Left(_) =>
            log.error(s"Unable to decode event for persistence_id = '$persistenceId' and sequence_nr = '$sequenceNr'")
            Source.empty
        }
    }
    .runWith(sink)

  Await.result(done, Duration.Inf)
  log.info(s"Finished migration of '$keyspace.messages' table. Migrated ${migratedCount.get()} events.")

}

private def getOrganization(v1Event: Json, uuids: Map[String, String]): Option[String] = {
  v1Event.hcursor.get[String]("project") match {
    case Right(project) if uuids.contains(project) => Some(uuids(project))
    case Right(project) =>
      log.error(s"organization uuid could not be found for project '$project'")
      None
    case Left(err) =>
      log.error(s"'project' field could not be found on json '$v1Event'", err)
      None
  }
}
private def removeAlpha(v1Event: Json): Json =
  v1Event.hcursor.downField("types").withFocus(_.mapArray(_.filterNot(_ == Json.fromString(alphaType))))
    .up.downField("source").downField("@type").withFocus(_.mapArray(_.filterNot(_ == Json.fromString("Alpha")))).top.getOrElse(v1Event)


private def transformEvent(v1Event: Json, volume: String): Json = {
  val storage = v1Event.asObject.flatMap(_("storage"))
  storage match {
    case None =>
      val oldAttributes = v1Event.asObject.get("attributes").get.asObject.get
      val filePath      = oldAttributes("filePath").flatMap(_.asString).get
      val location      = s"file://$volume/$filePath"
      val newAttributes = oldAttributes.remove("filePath").add("path", Json.fromString(filePath)).add("location", Json.fromString(location))

      val projectUuid = v1Event.asObject.get("project").get.asString.get

      val storage = Json.obj(
        "ref"             -> Json.fromString(projectUuid),
        "id"              -> Json.fromString("https://bluebrain.github.io/nexus/vocabulary/diskStorageDefault"),
        "rev"             -> Json.fromInt(1),
        "deprecated"      -> Json.fromBoolean(false),
        "default"         -> Json.fromBoolean(true),
        "algorithm"       -> Json.fromString("SHA-256"),
        "volume"          -> Json.fromString(volume),
        "readPermission"  -> Json.fromString("resources/read"),
        "writePermission" -> Json.fromString("files/write"),
        "@type"           -> Json.fromString("DiskStorage")
      )
      v1Event.mapObject(_.add("attributes", Json.fromJsonObject(newAttributes)).add("storage", storage))
    case _ =>
      log.debug(s"Event '${v1Event.noSpaces}'already migrated")
      v1Event
  }
}