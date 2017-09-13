package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.Done
import akka.http.scaladsl.model.HttpMessage.DiscardedEntity
import akka.http.scaladsl.model.{HttpEntity, HttpMessage, HttpRequest, HttpResponse}
import akka.stream.Materializer
import akka.util.ByteString
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.RdfMediaTypes
import ch.epfl.bluebrain.nexus.kg.core.Resources
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class SparqlFixtures extends Resources {
  def fixedResponse(file: String, replacements: Map[String, String] = Map()) = HttpResponse(entity = HttpEntity.Strict(
    RdfMediaTypes.`application/sparql-results+json`,
    ByteString(contentOf(file, replacements))))
}

object SparqlFixtures extends SparqlFixtures {

  final case class Source(`@id`: String, links: List[Link])

  def fixedHttpClient(resp: HttpResponse)(implicit mt: Materializer, ec: ExecutionContext) =
    new UntypedHttpClient[Future] {
      override def apply(req: HttpRequest): Future[HttpResponse] =
        Future.successful(resp)

      override def discardBytes(entity: HttpEntity): Future[HttpMessage.DiscardedEntity] =
        Future.successful(new DiscardedEntity(Future.successful(Done)))

      override def toString(entity: HttpEntity): Future[String] =
        entity.toStrict(1 second).map(_.data.utf8String)
    }
}
