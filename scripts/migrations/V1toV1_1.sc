import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

import $ivy.`com.lightbend.akka:akka-stream-alpakka-cassandra_2.12:1.0-RC1`
import $ivy.`io.circe:circe-core_2.12:0.11.1`
import $ivy.`io.circe:circe-parser_2.12:0.11.1`
import $ivy.`com.typesafe.scala-logging:scala-logging_2.12:3.9.2`
import $ivy.`ch.qos.logback:logback-classic:1.2.3`
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
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

val log = Logger("V1ToV11Migration")

val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
root.setLevel(Level.INFO)

@main
  def main(volume: String = "/opt/binaries",
           cassandraContactPoint: String = "127.0.0.1",
           cassandraPort: Int = 9042,
           cassandraUsername: String = "",
           cassandraPassword: String = "",
           keyspace: String = "kg"): Unit = {

    implicit val system: ActorSystem    = ActorSystem()
    implicit val mat: ActorMaterializer = ActorMaterializer()
    implicit val session = Cluster.builder
      .addContactPoint(cassandraContactPoint)
      .withPort(cassandraPort)
      .withCredentials(cassandraUsername, cassandraPassword)
      .build
      .connect()

    log.info(s"Dropping table $keyspace.projections")
    session.execute(s"DROP TABLE IF EXISTS $keyspace.projections")
    migrateMessagesTable(keyspace, volume)
    migrateTagViewsTable(keyspace, volume)
  }

private def migrateMessagesTable(keyspace: String, volume: String)(implicit session: Session,
                                                                     mat: ActorMaterializer) = {

    log.info(s"Starting migration of '$keyspace.messages' table")
    val migratedCount = new AtomicLong()
    val stmt          = new SimpleStatement(s"SELECT * FROM $keyspace.messages").setFetchSize(20)

    val preparedStatement =
      session.prepare(
        s"UPDATE $keyspace.messages SET event = textasblob(?) WHERE persistence_id = ? AND  partition_nr = ? AND sequence_nr = ? AND timestamp = ? AND timebucket = ?")
    //#prepared-statement

    //#statement-binder
    val statementBinder =
      (parameters: (Object, Object, Object, Object, Object, String), statement: PreparedStatement) =>
        parameters match {
          case (persistenceId, partitionNr, sequenceNr, timestamp, timebucket, event) =>
            statement.bind(event, persistenceId, partitionNr, sequenceNr, timestamp, timebucket)
      }

    val sink = CassandraSink[(Object, Object, Object, Object, Object, String)](parallelism = 1,
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
              val tpe = json.asObject.flatMap(_("@type")).flatMap(_.asString)

              tpe match {
                case Some("FileCreated") | Some("FileUpdated") =>
                  Source.single(
                    (persistenceId,
                     partitionNr,
                     sequenceNr,
                     timestamp,
                     timebucket,
                     transformEvent(json, volume, migratedCount).noSpaces))
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
private def migrateTagViewsTable(keyspace: String, volume: String)(implicit session: Session,
                                                                     mat: ActorMaterializer) = {
    log.info(s"Starting migration of '$keyspace.tag_views' table")
    val migratedCount = new AtomicLong()
    val stmt          = new SimpleStatement(s"SELECT * FROM $keyspace.tag_views").setFetchSize(20)

    val preparedStatement =
      session.prepare(
        s"UPDATE $keyspace.tag_views SET event = textasblob(?) WHERE tag_name = ? AND  timebucket = ? AND timestamp = ? AND persistence_id = ? AND tag_pid_sequence_nr = ?")
    //#prepared-statement

    //#statement-binder
    val statementBinder =
      (parameters: (Object, Object, Object, Object, Object, String), statement: PreparedStatement) =>
        parameters match {
          case (tagName, timebucket, timestamp, persistenceId, tagPidSequenceNr, event) =>
            statement.bind(event, tagName, timebucket, timestamp, persistenceId, tagPidSequenceNr)
      }

    val sink = CassandraSink[(Object, Object, Object, Object, Object, String)](parallelism = 1,
                                                                               preparedStatement,
                                                                               statementBinder)

    val done = CassandraSource(stmt)
      .map(
        row =>
          (row.getObject("tag_name"),
           row.getObject("timebucket"),
           row.getObject("timestamp"),
           row.getObject("persistence_id"),
           row.getObject("tag_pid_sequence_nr"),
           StandardCharsets.UTF_8.decode(row.get[ByteBuffer]("event", TypeCodec.blob())).toString))
      .flatMapConcat {
        case (tagName, timebucket, timestamp, persistenceId, tagPidSequenceNr, eventString) =>
          parse(eventString) match {
            case Right(json) =>
              val tpe = json.asObject.flatMap(_("@type")).flatMap(_.asString)

              tpe match {
                case Some("FileCreated") | Some("FileUpdated") =>
                  Source.single(
                    (tagName,
                     timebucket,
                     timestamp,
                     persistenceId,
                     tagPidSequenceNr,
                     transformEvent(json, volume, migratedCount).noSpaces))
                case _ => Source.empty
              }
            case Left(err) =>
              log.error(
                s"Unable to decode event for persistence_id = '$persistenceId' and sequence_nr = '$tagPidSequenceNr'")
              Source.empty
          }
      }
      .runWith(sink)

    Await.result(done, Duration.Inf)
    log.info(s"Finished migration of '$keyspace.tag_views' table. Migrated ${migratedCount.get()} events.")
  }

private def transformEvent(v1Event: Json, volume: String, counter: AtomicLong): Json = {
    val storage = v1Event.asObject.flatMap(_("storage"))
    storage match {
      case None =>
        val oldAttributes = v1Event.asObject.get("attributes").get.asObject.get
        val filePath      = oldAttributes("filePath").flatMap(_.asString).get
        val location      = s"file://$volume/$filePath"
        val newAttributes = oldAttributes.remove("filePath").add("location", Json.fromString(location))

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

        counter.incrementAndGet()
        v1Event.mapObject(_.add("attributes", Json.fromJsonObject(newAttributes)).add("storage", storage))
      case _ =>
        log.debug(s"Event '${v1Event.noSpaces}'already migrated")
        v1Event
    }
  }