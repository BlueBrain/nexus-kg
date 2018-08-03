package ch.epfl.bluebrain.nexus.kg.persistence

import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import monix.eval.Task

import scala.concurrent.Future

object TaskAggregate {

  /**
    * Creates a [[monix.eval.Task]] based aggregate from a [[scala.concurrent.Future]] one.
    *
    * @param agg the underlying future based aggregate
    */
  // $COVERAGE-OFF$
  final def fromFuture[Id, Ev, St, Cmd, Rej](
      agg: Aggregate.Aux[Future, Id, Ev, St, Cmd, Rej]): Aggregate.Aux[Task, Id, Ev, St, Cmd, Rej] =
    new Aggregate[Task] {
      override type Identifier = Id
      override type Event      = Ev
      override type State      = St
      override type Command    = Cmd
      override type Rejection  = Rej

      override def name: String = agg.name

      override def append(id: Id, event: Ev): Task[Long] =
        Task.deferFuture(agg.append(id, event))

      override def lastSequenceNr(id: Id): Task[Long] =
        Task.deferFuture(agg.lastSequenceNr(id))

      override def foldLeft[B](id: Id, z: B)(f: (B, Ev) => B): Task[B] =
        Task.deferFuture(agg.foldLeft(id, z)(f))

      override def currentState(id: Id): Task[St] =
        Task.deferFuture(agg.currentState(id))

      override def eval(id: Id, cmd: Cmd): Task[Either[Rej, St]] =
        Task.deferFuture(agg.eval(id, cmd))

      override def checkEval(id: Id, cmd: Cmd): Task[Option[Rej]] =
        Task.deferFuture(agg.checkEval(id, cmd))
    }
  // $COVERAGE-ON$
}
