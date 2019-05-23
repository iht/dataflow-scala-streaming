package dev.herraiz.meetup.dataflow.data

import dev.herraiz.meetup.dataflow.data.DataTypes.{PointTaxiRide, json2TaxiRide}
import io.circe
import org.joda.time.{Duration, Instant}

object TestData {

  val PIPELINE_START: Int = 10

  val jsonStr1 =
    """
      |{
      |  "ride_id": "0878437d-8dae-4ebc-b4cf-3a3da40396ad",
      |  "point_idx": 160,
      |  "latitude": 40.780210000000004,
      |  "longitude": -73.97139,
      |  "timestamp": "2019-09-09T11:42:48.33739-04:00",
      |  "meter_reading": 5.746939,
      |  "meter_increment": 0.03591837,
      |  "ride_status": "pickup",
      |  "passenger_count": 2
      |}
      |""".stripMargin

  val obj1: Either[circe.Error, PointTaxiRide] = json2TaxiRide(jsonStr1)

  val jsonStr2 =
    """
      |{
      |  "ride_id": "0878437d-8dae-4ebc-b4cf-3a3da40396ad",
      |  "point_idx": 161,
      |  "latitude": 40.180210000000004,
      |  "longitude": -73.17139,
      |  "timestamp": "2019-09-09T11:52:48.33739-04:00",
      |  "meter_reading": 6,
      |  "meter_increment": 0.03591837,
      |  "ride_status": "dropoff",
      |  "passenger_count": 2
      |}
      |""".stripMargin

  val obj2: Either[circe.Error, PointTaxiRide] = json2TaxiRide(jsonStr2)

  val badJson =
    """"{
      | "hello": "world"
      | }
      |""".stripMargin

  val baseTime: Instant = obj1.right.get
    .timestamp.minus(Duration.standardSeconds(PIPELINE_START))
}
