package ch.epfl.bluebrain.nexus.kg

import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.kg.DeprecatedId._
import ch.epfl.bluebrain.nexus.kg.resources._
import monix.eval.Task

package object routes {

  private[routes] implicit def toProjectLabel(implicit project: Project): ProjectLabel =
    ProjectLabel(project.organizationLabel, project.label)

  private[routes] def filterDeprecated[A: DeprecatedId](set: Task[Set[A]], deprecated: Option[Boolean]): Task[List[A]] =
    set.map(s => deprecated.map(d => s.filter(_.deprecated == d)).getOrElse(s).toList)

  private[routes] def toQueryResults[A](resolvers: List[A]): QueryResults[A] =
    UnscoredQueryResults(resolvers.size.toLong, resolvers.map(UnscoredQueryResult(_)))

}
