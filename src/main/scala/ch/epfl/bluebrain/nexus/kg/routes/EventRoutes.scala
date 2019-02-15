package ch.epfl.bluebrain.nexus.kg.routes

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.javadsl.server.Rejections.validationRejection
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
import akka.http.scaladsl.model.headers.`Last-Event-ID`
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, Route}
import akka.persistence.query._
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.stream.scaladsl.Source
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller, Permission}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.resources.Event
import ch.epfl.bluebrain.nexus.kg.resources.Event.JsonLd._
import io.circe.syntax._
import io.circe.{Encoder, Printer}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class EventRoutes(acls: AccessControlLists, caller: Caller)(implicit as: ActorSystem,
                                                            project: Project,
                                                            config: AppConfig) {

  private val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

  private val pq: EventsByTagQuery =
    PersistenceQuery(as).readJournalFor[EventsByTagQuery](config.persistence.queryJournalPlugin)

  private val resourcesRead: Set[Permission]    = Set(Permission.unsafe("resources/read"))
  private implicit val acl: AccessControlLists  = acls
  private implicit val c: Caller                = caller
  private implicit val iamConf: IamClientConfig = config.iam.iamClient

  def routes: Route =
    lastEventId { offset =>
      hasPermissions(resourcesRead).apply {
        complete(source(s"project=${project.uuid}", offset, eventToSse))
      }
    }

  protected def source(
      tag: String,
      offset: Offset,
      toSse: EventEnvelope => Option[ServerSentEvent]
  ): Source[ServerSentEvent, NotUsed] =
    pq.eventsByTag(tag, offset)
      .flatMapConcat(ee => Source(toSse(ee).toList))
      .keepAlive(10 seconds, () => ServerSentEvent.heartbeat)

  private def lastEventId: Directive1[Offset] =
    optionalHeaderValueByName(`Last-Event-ID`.name)
      .map(_.map(id => `Last-Event-ID`(id)))
      .flatMap {
        case Some(header) =>
          Try[Offset](TimeBasedUUID(UUID.fromString(header.id))) orElse Try(Sequence(header.id.toLong)) match {
            case Success(value) => provide(value)
            case Failure(_)     => reject(validationRejection("The value of the `Last-Event-ID` header is not valid."))
          }
        case None => provide(NoOffset)
      }

  private def aToSse[A: Encoder](a: A, offset: Offset): ServerSentEvent = {
    import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
    val json = a.asJson.sortKeys(AppConfig.orderedKeys)
    ServerSentEvent(
      data = json.pretty(printer),
      eventType = json.hcursor.get[String]("@type").toOption,
      id = offset match {
        case NoOffset            => None
        case Sequence(value)     => Some(value.toString)
        case TimeBasedUUID(uuid) => Some(uuid.toString)
      }
    )
  }

  private def eventToSse(envelope: EventEnvelope): Option[ServerSentEvent] =
    envelope.event match {
      case value: Event => Some(aToSse(value, envelope.offset))
      case _            => None
    }

}
