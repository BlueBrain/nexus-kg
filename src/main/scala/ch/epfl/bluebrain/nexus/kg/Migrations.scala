package ch.epfl.bluebrain.nexus.kg

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{UUID, Arrays => JArrays, HashSet => JHashSet, Set => JSet}

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.stream.scaladsl.Source
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticSearchView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import com.datastax.driver.core.TypeCodec
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.CanBlock

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
  * Migrate messages table in between minor Nexus revisions.
  */
object Migrations {
  // $COVERAGE-OFF$
  object V1ToV11 {

    private val alpha = "https://bluebrain.github.io/nexus/vocabulary/Alpha"

    private final case class Project(uuid: UUID, orgUuid: UUID, base: AbsoluteIri, vocab: AbsoluteIri)

    def migrate(c: AppConfig)(implicit as: ActorSystem, s: Scheduler, permit: CanBlock): Unit = {
      val log = Logger("V1 ====> V1.1")
      val journal = new CassandraReadJournal(
        as.asInstanceOf[ExtendedActorSystem],
        as.settings.config.getConfig("cassandra-query-journal")
      )
      val session = journal.session
      val admin   = sys.env.getOrElse("ADMIN_KEYSPACE", "admin")
      val kg      = journal.config.keyspace

      def truncateAndDropTables(): Unit = {
        val truncateTables = Set(
          "metadata",
          "tag_views",
          "tag_scanning",
          "tag_write_progress"
        )
        val dropTables = Set(
          "projections_progress",
          "projections_failures",
          "projections",
          "index_failures"
        )
        truncateTables.foreach { tableName =>
          log.info(s"Truncating table $kg.$tableName")
          try {
            session.executeWrite(s"TRUNCATE TABLE $kg.$tableName").runSyncDiscard()
          } catch {
            case NonFatal(_) => // ignore
              log.debug("Table truncation failed... the table may not exist yet.")
          }
        }
        dropTables.foreach { tableName =>
          log.info(s"Dropping table $kg.$tableName")
          try {
            session.executeWrite(s"DROP TABLE $kg.$tableName").runSyncDiscard()
          } catch {
            case NonFatal(_) => // ignore
              log.debug("Table drop failed... the table may not exist.")
          }
        }
      }

      def getProject(event: Json): Option[UUID] =
        event.hcursor.get[UUID]("project").toOption

      def loadProjects(): Map[UUID, Project] = {
        log.info("Loading project information.")
        val stmt   = s"SELECT persistence_id, event FROM $admin.messages WHERE event_manifest='project' ALLOW FILTERING"
        val source = session.prepare(stmt).map(prepared => session.select(prepared.bind())).runSync()
        val jsonMap = source
          .map { row =>
            val pid   = row.getString("persistence_id")
            val event = StandardCharsets.UTF_8.decode(row.get[ByteBuffer]("event", TypeCodec.blob())).toString
            (pid, event)
          }
          .flatMapConcat {
            case (persistenceId, eventString) =>
              parse(eventString) match {
                case Right(json) =>
                  val result = for {
                    projUuid <- json.hcursor.get[UUID]("id")
                    rev      <- json.hcursor.get[Option[Long]]("rev").map(_.getOrElse(1L))
                  } yield (projUuid, rev)

                  result match {
                    case Right((uuid, rev)) => Source.single((uuid, rev, json))
                    case Left(err) =>
                      log.error(s"Unable to decode project event '$json'", err)
                      Source.empty
                  }
                case Left(err) =>
                  log.error(s"Unable to parse event for persistence_id = '$persistenceId'", err)
                  Source.empty
              }
          }
          .runFold(Map.empty[UUID, Vector[(Long, Json)]]) {
            case (map, (uuid, rev, json)) =>
              val next = map.getOrElse(uuid, Vector.empty) :+ (rev -> json)
              map.updated(uuid, next)
          }
          .runSync()

        // some gymnastics on the project events to pull the necessary information
        val result = jsonMap.foldLeft(Map.empty[UUID, Project]) {
          case (projectMap, (projUuid, vector)) =>
            val sorted = vector.sortBy(_._1).map(_._2) // sort by revision and map to json repr
            val orgUuid = (for {
              created <- sorted.headOption
              value   <- created.hcursor.get[UUID]("organizationUuid").toOption
            } yield value).toRight(s"Unable to read the organizationUuid from project '$projUuid'")
            val base = sorted
              .foldLeft[Option[AbsoluteIri]](None) {
                case (acc, json) => json.hcursor.get[AbsoluteIri]("base").toOption orElse acc
              }
              .toRight(s"Unable to read the base value from project '$projUuid'")
            val vocab = sorted
              .foldLeft[Option[AbsoluteIri]](None) {
                case (acc, json) => json.hcursor.get[AbsoluteIri]("vocab").toOption orElse acc
              }
              .toRight(s"Unable to read the vocab value from project '$projUuid'")

            val proj = for {
              o <- orgUuid
              b <- base
              v <- vocab
            } yield Project(projUuid, o, b, v)

            proj match {
              case Left(err) =>
                log.error(err)
                projectMap // drop the current value
              case Right(value) =>
                projectMap.updated(projUuid, value)
            }
        }
        log.info("Finished loading project information.")
        result
      }

      def removeAlpha(event: Json): Json =
        event.hcursor
          .downField("types")
          .withFocus(_.mapArray(_.filterNot(_ == Json.fromString(alpha))))
          .up
          .downField("source")
          .downField("@type")
          .withFocus(_.mapArray(_.filterNot(_ == Json.fromString("Alpha"))))
          .top
          .getOrElse(event)

      def addOrg(event: Json, projects: Map[UUID, Project]): Json = {
        event.hcursor.get[UUID]("organization").toOption match {
          case Some(_) => event // event already migrated, skip
          case None =>
            val orgJson = for {
              projectUuid <- getProject(event)
              orgUuid     <- projects.get(projectUuid).map(_.orgUuid)
            } yield Json.obj("organization" -> Json.fromString(orgUuid.toString))
            orgJson match {
              case Some(value) => event deepMerge value
              case None =>
                log.error(s"Unable to extract org uuid for event '${event.noSpaces}'")
                event
            }
        }
      }

      def replaceDefaultView(event: Json, genView: ProjectRef => View): Json =
        getProject(event) match {
          case Some(projectUuid) =>
            val view: View = genView(ProjectRef(projectUuid))
            view.as[Json](viewCtx.appendContextOf(resourceCtx)).toOption match {
              case Some(json) =>
                val source = json.removeKeys(nxv.rev.prefix, nxv.deprecated.prefix).replaceContext(viewCtxUri)
                event deepMerge Json.obj("source" -> source)
              case _ =>
                log.error(s"Unable to create default view for project '$projectUuid'")
                event
            }
          case None =>
            log.error(s"Unable to find the project uuid form the event '${event.noSpaces}'")
            event
        }

      def replaceDefaultEsView(event: Json): Json = replaceDefaultView(event, ElasticSearchView.default)

      def replaceDefaultSparqlView(event: Json): Json = replaceDefaultView(event, SparqlView.default)

      def orgOf(event: Json): Option[UUID] = {
        event.hcursor.get[UUID]("organization").toOption
      }

      def addCtxIfEmpty(event: Json, projects: Map[UUID, Project]): Json = {
        val hasSource = event.hcursor.get[Json]("source").isRight
        if (hasSource) {
          val extracted = for {
            ctxValue    <- event.hcursor.get[Json]("source").map(_.contextValue).toOption
            projectUuid <- getProject(event)
            project     <- projects.get(projectUuid)
            base  = project.base.asJson
            vocab = project.vocab.asJson
          } yield (ctxValue, base, vocab)
          extracted match {
            case Some((ctx, base, vocab)) if ctx == Json.obj() =>
              val newCtx = Json.obj("source" -> Json.obj("@context" -> Json.obj("@base" -> base, "@vocab" -> vocab)))
              event deepMerge newCtx
            case Some(_) => // event already contains context, skipping
              event
            case None =>
              log.error(s"Unable to extract context and / or project for event '$event'")
              event
          }
        } else event
      }

      def transformStorageEvent(event: Json, types: Set[String]): Json = {

        def transformSource(source: Json): Json = event deepMerge Json.obj("source" -> source)

        event.hcursor.downField("source").focus.flatMap(_.asObject) match {
          case Some(source) if types.contains(nxv.DiskStorage.prefix) && !source.contains(nxv.maxFileSize.prefix) =>
            transformSource(source.add(nxv.maxFileSize.prefix, c.storage.disk.maxFileSize.asJson).asJson)
          case Some(source)
              if types.contains(nxv.RemoteDiskStorage.prefix) && !source.contains(nxv.maxFileSize.prefix) =>
            transformSource(source.add(nxv.maxFileSize.prefix, c.storage.remoteDisk.maxFileSize.asJson).asJson)
          case Some(source) if types.contains(nxv.S3Storage.prefix) && !source.contains(nxv.maxFileSize.prefix) =>
            transformSource(source.add(nxv.maxFileSize.prefix, c.storage.amazon.maxFileSize.asJson).asJson)
          case Some(source) if source.contains(nxv.maxFileSize.prefix) => event
          case Some(_) =>
            log.error(s"Unable to extract any storage type from the event '$event'. Found types: '$types'")
            event
          case _ =>
            log.error(s"Unable to extract the 'source' from the event '$event'")
            event
        }
      }

      def transformFileEvent(event: Json, volume: String, projects: Map[UUID, Project]): Json = {
        event.hcursor.get[Json]("storage").toOption match {
          case Some(_) => event // event already migrated, skip
          case None =>
            val newAttributes = for {
              currentAttributes   <- event.hcursor.get[Json]("attributes")
              currentFilePath     <- currentAttributes.hcursor.get[String]("filePath")
              newAttributesNoPath <- currentAttributes.hcursor.downField("filePath").delete.as[Json]
              location  = Json.fromString(s"file://$volume/$currentFilePath")
              path      = Json.fromString(currentFilePath)
              newFields = Json.obj("location" -> location, "path" -> path)
            } yield newAttributesNoPath.deepMerge(newFields)
            val storage = Json.obj(
              "storage" -> Json.obj(
                "id"    -> Json.fromString("https://bluebrain.github.io/nexus/vocabulary/diskStorageDefault"),
                "rev"   -> Json.fromLong(1L),
                "@type" -> Json.fromString("DiskStorageReference")
              )
            )
            newAttributes match {
              case Left(err) =>
                log.error("Unable to transform file event", err)
                event
              case Right(value) =>
                val noAttributesEvent   = event.hcursor.downField("attributes").delete.top.getOrElse(event)
                val justAttributesEvent = Json.obj("attributes" -> value)
                addOrg(noAttributesEvent deepMerge justAttributesEvent deepMerge storage, projects)
            }
        }
      }

      def transformNonFileEvent(event: Json, projects: Map[UUID, Project]): Json = {
        val nonAlpha      = removeAlpha(event)
        val withOrg       = addOrg(nonAlpha, projects)
        val withOrgAndCtx = addCtxIfEmpty(withOrg, projects)

        val optId    = event.hcursor.get[String]("id").toOption
        val optTypes = event.hcursor.downField("source").get[Set[String]]("@type").toOption

        optId -> optTypes match {
          case (_, Some(types)) if types.contains(nxv.Storage.prefix) =>
            transformStorageEvent(withOrgAndCtx, types)
          case (Some(id), Some(types))
              if types.contains(nxv.ElasticSearchView.prefix) && id == nxv.defaultElasticSearchIndex.value.asString =>
            replaceDefaultEsView(withOrgAndCtx)
          case (Some(id), Some(types))
              if types.contains(nxv.SparqlView.prefix) && id == nxv.defaultSparqlIndex.value.asString =>
            replaceDefaultSparqlView(withOrgAndCtx)
          case _ =>
            withOrgAndCtx
        }
      }

      def migrateMessagesTable(volume: String, projects: Map[UUID, Project]): Unit = {
        val allEvents = s"SELECT * FROM $kg.messages"
        val updateEvent =
          s"UPDATE $kg.messages SET event = textasblob(?), tags = tags + ?, tags = tags - {'type=$alpha'} WHERE persistence_id = ? AND  partition_nr = ? AND sequence_nr = ? AND timestamp = ? AND timebucket = ?"
        val source = session.prepare(allEvents).map(prepared => session.select(prepared.bind())).runSync()
        source
          .flatMapConcat { row =>
            Try {

              (
                row.getObject("persistence_id"),
                row.getObject("partition_nr"),
                row.getObject("sequence_nr"),
                row.getObject("timestamp"),
                row.getObject("timebucket"),
                StandardCharsets.UTF_8.decode(row.get[ByteBuffer]("event", TypeCodec.blob())).toString
              )
            } match {
              case Success(value) => Source.single(value)
              case Failure(NonFatal(th)) =>
                try {
                  log.error(
                    s"Failed to read event for pid '${row.getObject("persistence_id")}', seq '${row.getObject("sequence_nr")}'",
                    th
                  )
                } catch {
                  case NonFatal(_) => log.error("Failed to read unknown row in messages table.")
                }
                Source.empty
            }
          }
          .flatMapConcat {
            case (pid, partitionNr, seqNr, timestamp, timebucket, eventString) =>
              parse(eventString) match {
                case Right(event) =>
                  event.hcursor.get[String]("@type").toOption match {
                    case Some("FileCreated") | Some("FileUpdated") =>
                      val migrated = transformFileEvent(event, volume, projects)
                      orgOf(migrated) match {
                        case Some(org) =>
                          Source.single((pid, partitionNr, seqNr, timestamp, timebucket, org, migrated.noSpaces))
                        case None =>
                          log.error(s"Unable to extract org uuid for migrated event '$migrated'")
                          Source.empty
                      }
                    case _ =>
                      val migrated = transformNonFileEvent(event, projects)
                      orgOf(migrated) match {
                        case Some(org) =>
                          Source.single((pid, partitionNr, seqNr, timestamp, timebucket, org, migrated.noSpaces))
                        case None =>
                          log.error(s"Unable to extract org uuid for migrated event '$migrated'")
                          Source.empty
                      }
                  }
                case Left(err) =>
                  log.error(s"Failed to parse event '$eventString' to json", err)
                  Source.empty
              }
          }
          .mapAsync(1) {
            case (pid, partitionNr, seqNr, timestamp, timebucket, org, eventString) =>
              session.executeWrite(
                updateEvent,
                eventString,
                new JHashSet(JArrays.asList(s"org=${org.toString}")): JSet[String],
                pid,
                partitionNr,
                seqNr,
                timestamp,
                timebucket
              )
          }
          .runFold(0) {
            case (acc, _) =>
              if (acc % 1000 == 0) log.info(s"Processed '$acc' messages.")
              acc + 1
          }
          .map(_ => ())
          .runSyncDiscard()
      }

      log.info("Migrating messages table.")
      truncateAndDropTables()
      val projects = loadProjects()
      migrateMessagesTable(c.storage.disk.volume.toAbsolutePath.toString, projects)
      log.info("Migration complete.")
    }
  }
  implicit class RichFuture[A](val future: Future[A]) extends AnyVal {
    def runSync()(implicit s: Scheduler, permit: CanBlock): A =
      Task.fromFuture(future).runSyncUnsafe()
    def runSyncDiscard()(implicit s: Scheduler, permit: CanBlock): Unit =
      Task.fromFuture(future).map(_ => ()).runSyncUnsafe()
  }
  // $COVERAGE-ON$
}
