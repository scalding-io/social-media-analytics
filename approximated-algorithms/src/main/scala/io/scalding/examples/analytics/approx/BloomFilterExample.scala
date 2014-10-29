package io.scalding.examples.analytics.approx

import com.twitter.scalding.typed.TDsl
import com.twitter.scalding._
import TDsl._
import com.twitter.algebird.BloomFilter

case class BigFileElem(key: String, strValue: String, intValue: Int)
case class SmallFileElem(key: String, strValue: String, intValue: Int, joinKey: String)

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

object Incident {
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

object SkipHeaderTypedCsv extends TypedSeperatedFile {
  override val separator = ","
  override val skipHeader = true
}

/**
 * Given the Incidents of a specific day (daily argument) and the historical one to join to (historical argument)
 * returns all daily incidents together with the historical one of the same category in the same police district
 * grouped by category and district
 */
class BloomFilterExample(args: Args) extends Job(args) {

  val size = args.optional("estimatedSize") map { _.toInt } getOrElse 100000
  val fpProb = args.optional("accuracy") map { _.toDouble } getOrElse 0.01d

  val historicalIncidents = SkipHeaderTypedCsv[Incident.IncidentTuple](args("historical")) map { Incident.fromTuple }
  val dailyIncidents = SkipHeaderTypedCsv[Incident.IncidentTuple](args("daily")) map { Incident.fromTuple }

  implicit val bloomFilterMonoid = BloomFilter(size, fpProb)

  val matchingDailyDataSetBloomFilter =
    dailyIncidents
      .map { incident =>
        bloomFilterMonoid.create(incidentFilterKey(incident))
      }
      .sum


  val historicalMatchingDailyApprox =
    historicalIncidents
      .filterWithValue(matchingDailyDataSetBloomFilter) { (incident, bloomFilterOp) =>
        bloomFilterOp.map { filter => filter.contains( incidentFilterKey(incident) ).isTrue } getOrElse false
      }

  val historicalMatchingDaily =
    historicalMatchingDailyApprox
      .groupBy( incidentFilterKey )
      .rightJoin( dailyIncidents.groupBy( incidentFilterKey ) )
      .mapValues( _._1 )
      .values
      .filter( _.isDefined )
      .map { _.get }

  val joined = dailyIncidents ++ historicalMatchingDaily

  joined
    .groupBy( incidentFilterKey )
    .values
    .map {
      incident => (incident.category, incident.pdDistrict, incident.incidntNum, incident.date, incident.time, incident.descript, incident.resolution)
    }
    .toPipe('category, 'pdDistrict, 'incidntNum, 'date, 'time, 'descript, 'resolution)
    .write( Csv(args("output")) )

  
  def incidentFilterKey(incident: Incident): String = s"${incident.category}|${incident.pdDistrict}"
}
