package io.scalding.examples.analytics.approx

object Incident {
  val fields = List('incidntNum, 'category, 'descript, 'dayOfWeek, 'date, 'time,
    'pdDistrict, 'resolution, 'address, 'locX, 'locY, 'location)

  type IncidentTuple = (Long, String, String, String, String, String, String, String, String, String, String, String)

  def fromTuple(tuple: IncidentTuple) =
    Incident(
      tuple._1,
      tuple._2,
      tuple._3,
      tuple._4,
      tuple._5,
      tuple._6,
      tuple._7,
      tuple._8,
      tuple._9,
      tuple._10,
      tuple._11,
      tuple._12
    )
}

case class Incident(
   incidntNum: Long,
   category: String,
   descript: String,
   dayOfWeek: String,
   date: String,
   time: String,
   pdDistrict: String,
   resolution: String,
   address: String,
   locX: String,
   locY: String,
   location: String
)

