package io.scalding.approximations.model

object Wikipedia {
  type WikipediaType = (Long, String, Long ,String)
  def fromTuple(t: WikipediaType): Wikipedia = Wikipedia(t._1, t._2, t._3, t._4)
}
case class Wikipedia(ContributorID: Long, ContributorUserName: String, RevisionID: Long, DateTime: String)