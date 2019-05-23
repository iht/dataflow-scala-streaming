package dev.herraiz.meetup.dataflow.data

import org.scalatest._
import TestData._
import dev.herraiz.meetup.dataflow.data.DataTypes.TaxiRide


class TaxiRideSpec extends WordSpec with Matchers {

  val ptr: DataTypes.PointTaxiRide = obj1.right.get

  "The toTaxiRide method" should {
    "produce a correct object" in {
      val tr: TaxiRide = ptr.toTaxiRide
      tr shouldBe a[TaxiRide]
      tr.ride_id shouldBe ptr.ride_id
      tr.init shouldBe ptr.timestamp
      tr.finish shouldBe None
      tr.init_status shouldBe ptr.ride_status
      tr.finish_status shouldBe None
      tr.n_points shouldBe 1
      tr.total_meter shouldBe ptr.meter_increment
    }
  }

  "Two TaxiRides" should {
    "be aggregated correcly" in {
      val tr1 = ptr.toTaxiRide
      val tr2 = ptr.toTaxiRide
      val tr = tr1 + tr2
      tr shouldBe a[TaxiRide]
      tr.ride_id shouldBe tr1.ride_id
      tr.ride_id shouldBe tr2.ride_id
      tr.init shouldBe tr1.init
      tr.finish shouldBe Some(tr1.init)
      tr.init_status shouldBe tr1.init_status
      tr.finish_status shouldBe Some(tr1.init_status)
      tr.n_points shouldBe 2
      tr.total_meter shouldBe tr1.total_meter + tr2.total_meter
    }
  }
}
