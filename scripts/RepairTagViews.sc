import java.nio.file.Paths

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.akka:akka-http_2.12:10.1.8`
import $ivy.`com.typesafe.scala-logging:scala-logging_2.12:3.9.2`
import $ivy.`de.heikoseeberger:akka-http-circe_2.12:1.25.2`
import $ivy.`io.circe:circe-core_2.12:0.11.1`
import $ivy.`io.circe:circe-generic_2.12:0.11.1`
import $ivy.`io.circe:circe-parser_2.12:0.11.1`
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, Authorization, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling.PredefinedFromEntityUnmarshallers
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import akka.stream.scaladsl.{FileIO, Framing, Sink, Source}
import akka.util.ByteString
import ch.qos.logback.classic.Level
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser.parse
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

val log = Logger("TagViewRepair")
val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
root.setLevel(Level.INFO)

@main
  def main(kgBase: String,
           adminBase: String,
           token: String,
           persistenceIdsFile: String,
           parallelRequests: Int = 5): Unit = {

    implicit val system: ActorSystem    = ActorSystem()
    implicit val mat: ActorMaterializer = ActorMaterializer()
    implicit val ec: ExecutionContext   = system.dispatcher
    rebuildTagViews(kgBase, adminBase, token, persistenceIdsFile, parallelRequests)
  }

implicit val projectDecoder: Decoder[Project] = deriveDecoder
implicit val listingDecoder: Decoder[Listing] = deriveDecoder

private def rebuildTagViews(kgBase: String, adminBase: String, token: String, file: String, parallelRequests: Int)(
      implicit
      mat: ActorMaterializer,
      as: ActorSystem,
      ec: ExecutionContext) = {

    val headers: collection.immutable.Seq[HttpHeader] =
      collection.immutable.Seq(Authorization(OAuth2BearerToken(token)), Accept(MediaTypes.`application/json`))

    val projects = Source
      .unfoldAsync(0) { from =>
        Http()
          .singleRequest(HttpRequest(uri = s"$adminBase/projects?from=$from", headers = headers))
          .flatMap(res => PredefinedFromEntityUnmarshallers.stringUnmarshaller(res.entity))
          .map(parse(_).right.get)
          .map { json =>
            val listings = json.as[Listing].right.get
            if (listings._results.isEmpty)
              None
            else
              Some((from + 10, listings._results))
          }
      }
      .mapConcat[Project](_.to[collection.immutable.Seq])
      .runWith(Sink.seq)

    val projectLabels: Map[String, String] =
      Await.result(projects, Duration.Inf).map(p => p._uuid -> s"${p._organizationLabel}/${p._label}").toMap

    val sink = Sink.fold[Map[StatusCode, Long], StatusCode](Map.empty) { (counts, code) =>
      val prev    = counts.getOrElse(code, 0L)
      val updated = counts.updated(code, prev + 1)
      val total   = updated.values.sum
      if (total % 100 == 0)
        log.info(s"Processed $total instances: $updated")
      updated
    }
    val done = FileIO
      .fromPath(Paths.get(file))
      .via(Framing.delimiter(ByteString("\n"), 25600, false).map(_.utf8String))
      .map { id =>
        val resourceId   = id.trim.split("-").drop(6).mkString("-")
        val projectUuid  = id.split("-").slice(1, 6).mkString("-")
        val projectLabel = projectLabels(projectUuid)
        val url          = s"$kgBase/resources/$projectLabel/_/$resourceId"

        url
      }
      .mapAsync(parallelRequests) { url =>
        Http()
          .singleRequest(HttpRequest(uri = url, headers = headers))
          .flatMap { resp =>
            if (resp.status.intValue() != 200) {
              PredefinedFromEntityUnmarshallers
                .stringUnmarshaller(resp.entity)
                .map(ent => log.warn(s"Unexpected response from $url ${resp.status} body: '$ent'"))
                .map(_ => resp.status)
            } else {
              resp.discardEntityBytes().future().map(_ => resp.status)
            }
          }
      }
      .runWith(sink)

    val result         = Await.result(done, Duration.Inf)
    val totalProcessed = result.values.sum
    log.info(s"Processed $totalProcessed instances: $result")

  }

case class Listing(_total: Int, _results: List[Project])

case class Project(_organizationLabel: String, _label: String, _uuid: String)