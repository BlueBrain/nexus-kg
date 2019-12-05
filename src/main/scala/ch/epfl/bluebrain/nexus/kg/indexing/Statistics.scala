package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.Instant

import ch.epfl.bluebrain.nexus.kg.config.Contexts.statisticsCtxUri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import com.github.ghik.silencer.silent
import io.circe.Encoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder

/**
  * Representation of statistics.
  *
  * @param processedEvents            count of processed events
  * @param discardedEvents            count of events dropped
  * @param failedEvents               count of events failed
  * @param evaluatedEvents            count of events in the stream that have been used to update an index
  * @param remainingEvents            count of events still remaining to be processed
  * @param totalEvents                total number of events for the project
  * @param lastEventDateTime          datetime of the last event in the project
  * @param lastProcessedEventDateTime time of the last processed event in the project
  * @param delayInSeconds             indexing delay
  * @param nextRestart                next time when a restart is going to be triggered
  */
final case class Statistics(
    processedEvents: Long,
    discardedEvents: Long,
    failedEvents: Long,
    evaluatedEvents: Long,
    remainingEvents: Long,
    totalEvents: Long,
    lastEventDateTime: Option[Instant],
    lastProcessedEventDateTime: Option[Instant],
    delayInSeconds: Option[Long],
    nextRestart: Option[Instant]
)

object Statistics {

  /**
    * Create an instance of [[Statistics]].
    *
    * @param processedEvents            count of processed events
    * @param discardedEvents            count of events dropped
    * @param failedEvents               count of events failed
    * @param totalEvents                total number of events for the project
    * @param lastEventDateTime          datetime of the last event in the project
    * @param lastProcessedEventDateTime time of the last processed event in the project
    * @param nextRestart                next time when a restart is going to be triggered
    */
  def apply(
      processedEvents: Long,
      discardedEvents: Long,
      failedEvents: Long,
      totalEvents: Long,
      lastEventDateTime: Option[Instant],
      lastProcessedEventDateTime: Option[Instant],
      nextRestart: Option[Instant] = None
  ): Statistics =
    Statistics(
      processedEvents = processedEvents,
      totalEvents = totalEvents,
      discardedEvents = discardedEvents,
      failedEvents = failedEvents,
      remainingEvents = totalEvents - processedEvents,
      evaluatedEvents = processedEvents - discardedEvents - failedEvents,
      lastEventDateTime = lastEventDateTime,
      lastProcessedEventDateTime = lastProcessedEventDateTime,
      nextRestart = nextRestart,
      delayInSeconds = for {
        lastEvent          <- lastEventDateTime
        lastProcessedEvent <- lastProcessedEventDateTime
      } yield lastEvent.getEpochSecond - lastProcessedEvent.getEpochSecond
    )

  @silent // private implicits in automatic derivation are not recognized as used
  private implicit val config: Configuration = Configuration.default
  implicit val statisticsEncoder: Encoder[Statistics] = deriveConfiguredEncoder[Statistics]
    .mapJson(_.addContext(statisticsCtxUri))
}
