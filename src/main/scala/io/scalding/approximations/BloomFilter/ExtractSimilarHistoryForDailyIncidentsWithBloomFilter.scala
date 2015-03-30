package io.scalding.approximations.BloomFilter

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.twitter.algebird.BloomFilter
import com.twitter.scalding._
import com.twitter.scalding.typed.TDsl._

/**
 * Given the Incidents of a specific day (daily argument) and the historical one to join to (historical argument)
 * returns all daily incidents together with the historical one of the same category in the same police district
 * grouped by category and district
 *
 * @author Stefano Galarraga - http://scalding.io
 */
class ExtractSimilarHistoryForDailyIncidentsWithBloomFilter(args: Args) extends Job(args) {
  import incidentUtils._

  val output = args("output")
  val size = args.optional("estimatedSize") map { _.toInt } getOrElse 100000
  val fpProb = args.optional("accuracy") map { _.toDouble } getOrElse 0.01d

  val historicalIncidents = readIncidentFromCsv(args("historical"))
  val dailyIncidents = readIncidentFromCsv(args("daily"))

  val filteredHistorical = historicalIncidents approximatedMatchingWith (dailyIncidents, size, fpProb)

  val historicalMatchingDaily = filteredHistorical entriesMatchingWith dailyIncidents

  val joined = dailyIncidents ++ historicalMatchingDaily

  joined
    .extractGroupedProjection()
    .write( Csv(output) ) // results/similar-history-wit-BF"
}

/**
 * Same as ExtractSimilarHistoryForDailyIncidentsWithBloomFilter but without the first step of extracting candidate entries
 * with the Bloom Filter
 */
class ExtractSimilarHistoryForDailyIncidentsWithNoBloomFilter(args: Args) extends Job(args) with FieldConversions {
  import incidentUtils._

  val output = args("output")
  val historicalIncidents = readIncidentFromCsv(args("historical"))
  val dailyIncidents = readIncidentFromCsv(args("daily"))

  val historicalMatchingDaily = historicalIncidents entriesMatchingWith dailyIncidents

  val joined = dailyIncidents ++ historicalMatchingDaily

  joined
    .extractGroupedProjection()
    .write( Csv(output) ) // results/similar-history-without-BF
}

object incidentUtils extends FieldConversions {
  def incidentFilterKey(incident: Incident): String = s"${incident.category}|${incident.pdDistrict}"

  def readIncidentFromCsv(fileName: String, skipHeader: Boolean = true)(implicit flowDef: FlowDef, mode: Mode): TypedPipe[Incident] =
    Csv(fileName, fields = Incident.fields, skipHeader = skipHeader)
      .read
      .toTypedPipe[Incident.IncidentTuple](Incident.fields)
      .map { Incident.fromTuple }

  implicit class IncidentOps(val self: TypedPipe[Incident]) extends AnyVal {
    def entriesMatchingWith(matchWith: TypedPipe[Incident], matchingKeyExtractor: Incident => String = incidentFilterKey) : TypedPipe[Incident] = {
      self
        .groupBy( matchingKeyExtractor )
        .rightJoin( matchWith.groupBy( matchingKeyExtractor ) )
        .mapValues( _._1 )
        .values
        .filter( _.isDefined )
        .map { _.get }
    }

    def extractGroupedProjection(groupBy: Incident => String = incidentFilterKey)(implicit flowDef: FlowDef, mode: Mode): Pipe = {
      self
        .groupBy( groupBy )
        .values
        .map { incident => (incident.category, incident.pdDistrict, incident.incidntNum, incident.date, incident.time, incident.descript, incident.resolution) }
        .toPipe( 'category, 'pdDistrict, 'incidntNum, 'date, 'time, 'descript, 'resolution )
    }

    def approximatedMatchingWith(matchWith: TypedPipe[Incident], estimatedSize: Int, accuracy: Double, matchingKeyExtractor: Incident => String = incidentFilterKey) = {
      implicit val bloomFilterMonoid = BloomFilter(estimatedSize, accuracy)

      val matchingDailyDataSetBloomFilter =
        matchWith
          .map { incident =>
            bloomFilterMonoid.create(incidentFilterKey(incident))
          }
          .sum

      self
        .filterWithValue(matchingDailyDataSetBloomFilter) { (incident, bloomFilterOp) =>
        bloomFilterOp.exists { filter => filter.contains(incidentFilterKey(incident)).isTrue }
      }

    }

  }
}





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