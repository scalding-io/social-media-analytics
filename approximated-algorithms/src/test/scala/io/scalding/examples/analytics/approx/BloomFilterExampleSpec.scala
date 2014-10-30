package io.scalding.examples.analytics.approx

import com.twitter.scalding.{FieldConversions, Csv, TypedCsv, JobTest}
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable


class BloomFilterExampleSpec extends FlatSpec with Matchers with FieldConversions {

  def fakeIncident(index: Int, category: String, district: String) =
    (s"$index", category, "desc", "dayOfWeek", "date", "time", district, "resolution", "addres", "x", "y", "(x, y)")

  behavior of "BloomFilterExample"

  it should "Filter Elements not in Big File with some error" in {

    val size = 100
    val matchingHistorical = ((1 to size) map  { idx => fakeIncident(idx, s"category_${idx % 2}", s"district_${idx % 2}")  } )
    val nonMatchingHistorical = Seq(
      fakeIncident(90003, "category_1", "no_district"),
      fakeIncident(90004, "no_matching_category", "no_matching_district")
    )

    val historical =   matchingHistorical.toList ::: nonMatchingHistorical.toList

    val daily = List(
      fakeIncident(90001, "category_1", "district_1"),
      fakeIncident(90002, "category_2", "district_2")
    )

    JobTest( classOf[BloomFilterExample].getName )
      .arg("historical", "historical")
      .arg("daily", "daily")
      .arg("estimatedSize", size.toString)
      .arg("output", "output")
      .source(Csv("historical", fields = Incident.fields, skipHeader = true), historical)
      .source(Csv("daily", fields = Incident.fields, skipHeader = true), daily)
      .sink(Csv("output")) {
        buffer: mutable.Buffer[(String, String, Long, String, String, String, String)] =>
            buffer.toList.map { _._3 } should not contain (atLeastOneOf(90003, 90004))
      }
      .run
  }

}
