package io.scalding.approximations.model

object StackExchange {
  type StackExchangeType = (Long, Long, Long,Long, String, Long, Long, String,String)
  def fromTuple(t: StackExchangeType): StackExchange = StackExchange(t._1, t._2, t._3, t._4, t._5,t._6,t._7,t._8,t._9)
}
case class StackExchange(ID: Long, PostTypeID: Long, ParentID: Long, OwnerUserID: Long, CreationDate: String,
                         ViewCount: Long, FavoriteCount: Long, Tags: String, Keywords: String)
