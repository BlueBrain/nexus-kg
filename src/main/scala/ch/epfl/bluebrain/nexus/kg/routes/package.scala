package ch.epfl.bluebrain.nexus.kg

import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticDecoder
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{withTaskUnmarshaller, UntypedHttpClient}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ElasticDecoders.resourceIdDecoder
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

package object routes {

  private[routes] implicit def toProject(implicit value: LabeledProject): Project           = value.project
  private[routes] implicit def toProjectLabel(implicit value: LabeledProject): ProjectLabel = value.label

  private[routes] implicit def qrsClient(implicit httpClient: UntypedHttpClient[Task],
                                         mt: ActorMaterializer,
                                         wrapped: LabeledProject,
                                         http: HttpConfig): HttpClient[Task, QueryResults[AbsoluteIri]] = {
    implicit val elasticDec = ElasticDecoder(resourceIdDecoder)
    withTaskUnmarshaller[QueryResults[AbsoluteIri]]
  }

}
