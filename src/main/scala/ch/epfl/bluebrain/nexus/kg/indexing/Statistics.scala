package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.Instant

import ch.epfl.bluebrain.nexus.kg.config.Contexts.statisticsCtxUri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Encoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder

/**
  * Representation of statistics.
  *
  * @param processedEvents            count of processed events
  * @param discardedEvents            count of events dropped due to not matching selection criteria
  * @param evaluatedEvents            count of events in the stream that have been used to update an index
  * @param remainingEvents            count of events still remaining to be processed
  * @param totalEvents                total number of events for the project
  * @param lastEventDateTime          datetime of the last event in the project
  * @param lastProcessedEventDateTime time of the last processed event in the project
  * @param delayInSeconds             indexing delay
  */
final case class Statistics(
    processedEvents: Long,
    discardedEvents: Long,
    evaluatedEvents: Long,
    remainingEvents: Long,
    totalEvents: Long,
    lastEventDateTime: Option[Instant],
    lastProcessedEventDateTime: Option[Instant],
    delayInSeconds: Option[Long]
)

object Statistics {

  /**
    * Create an instance of [[Statistics]].
    *
    * @param processedEvents            count of processed events
    * @param discardedEvents            count of events dropped due to not matching selection criteria
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
  ): Statistics =
    Statistics(
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
  implicit val statisticsEncoder: Encoder[Statistics] = deriveConfiguredEncoder[Statistics]
    .mapJson(_.addContext(statisticsCtxUri))
}
