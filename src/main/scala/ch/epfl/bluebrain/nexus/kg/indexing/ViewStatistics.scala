package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.Instant

import ch.epfl.bluebrain.nexus.kg.config.Contexts.viewCtxUri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Encoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveEncoder

/**
  * Representation of view statistics.
  *
  * @param processedEvents            count of processed events
  * @param discardedEvents            count of events dropped due to not matching view selection criteria
  * @param evaluatedEvents            count of events in the stream that have been used to update an index
  * @param remainingEvents            count of events still remaining to be processed by the view
  * @param totalEvents                total number of events for the project
  * @param lastEventDateTime          datetime of the last event in the project
  * @param lastProcessedEventDateTime time of the last processed event in the project
  * @param delayInSeconds             indexing delay
  */
final case class ViewStatistics(
    processedEvents: Long,
    discardedEvents: Long,
    evaluatedEvents: Long,
    remainingEvents: Long,
    totalEvents: Long,
    lastEventDateTime: Option[Instant],
    lastProcessedEventDateTime: Option[Instant],
    delayInSeconds: Option[Long]
)

object ViewStatistics {

  /**
    * Create an instance of [[ViewStatistics]].
    *
    * @param processedEvents            count of processed events
    * @param discardedEvents            count of events dropped due to not matching view selection criteria
    * @param totalEvents                total number of events for the project
    * @param lastEventDateTime          datetime of the last event in the project
    * @param lastProcessedEventDateTime time of the last processed event in the project
    */
  def apply(
      processedEvents: Long,
      discardedEvents: Long,
      totalEvents: Long,
      lastEventDateTime: Option[Instant],
      lastProcessedEventDateTime: Option[Instant]
  ): ViewStatistics =
    ViewStatistics(
      processedEvents = processedEvents,
      totalEvents = totalEvents,
      discardedEvents = discardedEvents,
      remainingEvents = totalEvents - processedEvents,
      evaluatedEvents = processedEvents - discardedEvents,
      lastEventDateTime = lastEventDateTime,
      lastProcessedEventDateTime = lastProcessedEventDateTime,
      delayInSeconds = for {
        lastEvent          <- lastEventDateTime
        lastProcessedEvent <- lastProcessedEventDateTime
      } yield lastEvent.getEpochSecond - lastProcessedEvent.getEpochSecond
    )

  private implicit val config: Configuration = Configuration.default
  implicit val viewStatisticsEncoder: Encoder[ViewStatistics] = deriveEncoder[ViewStatistics]
    .mapJson(_.addContext(viewCtxUri))
}
