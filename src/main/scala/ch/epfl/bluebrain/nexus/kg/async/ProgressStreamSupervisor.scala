package ch.epfl.bluebrain.nexus.kg.async

import ch.epfl.bluebrain.nexus.sourcing.projections.{ProjectionProgress, StreamSupervisor}

/**
  * Stream supervisor for [[ProjectionProgress]] containing the progressId
  *
  * @param progressId the progressId
  * @param value      the stream supervisor
  */
final case class ProgressStreamSupervisor[F[_]](progressId: String, value: StreamSupervisor[F, ProjectionProgress])
