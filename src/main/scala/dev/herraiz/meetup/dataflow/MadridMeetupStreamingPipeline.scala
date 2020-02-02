// Copyright 2019 Israel Herraiz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dev.herraiz.meetup.dataflow

import io.circe
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.io.PubsubIO
import com.spotify.scio.values.{SCollection, WindowOptions}
import dev.herraiz.meetup.dataflow.data.DataTypes._
import org.apache.beam.sdk.transforms.windowing._
import org.joda.time.Duration

object MadridMeetupStreamingPipeline {

  val SESSION_GAP = 600
  val EARLY_RESULT = 10
  val LATENESS = 900

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc = scontext

    val pubsubTopic: String = opts("pubsub-topic")
    val goodTable = opts("output-table")
    val badTable = opts("errors-table")
    val accumTable = opts("accum-table")

    val messages: SCollection[String] = getMessagesFromPubSub(pubsubTopic)
    val (rides: SCollection[PointTaxiRide], writableErrors) = parseJSONStrings(messages)

    rides.saveAsBigQuery(Table.Spec(goodTable), WRITE_APPEND, CREATE_IF_NEEDED)
    writableErrors.saveAsBigQuery(Table.Spec(badTable), WRITE_APPEND, CREATE_IF_NEEDED)

    // Group by session with a max duration of 5 mins between events
    // Window options
    val wopts: WindowOptions = customWindowOptions
    val groupRides = groupRidesByKey(rides.map(_.toTaxiRide), wopts)
    groupRides.saveAsBigQuery(Table.Spec(accumTable), WRITE_APPEND, CREATE_IF_NEEDED)

    sc.run
  }

  def customWindowOptions: WindowOptions =
    WindowOptions(
      trigger = AfterWatermark.pastEndOfWindow()
        .withEarlyFirings(AfterProcessingTime
          .pastFirstElementInPane
          .plusDelayOf(Duration.standardSeconds(EARLY_RESULT)))
        .withLateFirings(AfterProcessingTime
          .pastFirstElementInPane()
          .plusDelayOf(Duration.standardSeconds(LATENESS))),
      accumulationMode = streaming.ACCUMULATING_FIRED_PANES,
      allowedLateness = Duration.standardSeconds(LATENESS)
    )

  def getMessagesFromPubSub(pubsubTopic: String)(implicit sc: ScioContext): SCollection[String] = {
    val pubsub =
      PubsubIO.apply[String](pubsubTopic, "ride_id", "timestamp")

    val params = PubsubIO.ReadParam.apply(false)

    /*_*/
    val messages = sc.read(pubsub)(params) /*_*/

    messages
  }

  def parseJSONStrings(messages: SCollection[String]):
  (SCollection[PointTaxiRide], SCollection[JsonError]) = {
    val pointRidesEither = messages.map(json2TaxiRide)

    val (errorsLeft: SCollection[Either[circe.Error, PointTaxiRide]],
    ridesRight: SCollection[Either[circe.Error, PointTaxiRide]]) =
      pointRidesEither.partition(_.isLeft)

    val errors: SCollection[circe.Error] = errorsLeft.map(_.left.get)
    val rides: SCollection[PointTaxiRide] = ridesRight.map(_.right.get)

    val errorsWritable: SCollection[JsonError] = errors.map(circeErrorToCustomError)

    (rides, errorsWritable)
  }

  def groupRidesByKey(rides: SCollection[TaxiRide], wopts: WindowOptions): SCollection[TaxiRide] = {
    val ridesByKey: SCollection[(String, TaxiRide)] = rides.keyBy(_.ride_id)
      .withSessionWindows(Duration.standardSeconds(SESSION_GAP), wopts)

    val summed: SCollection[TaxiRide] = ridesByKey.reduceByKey(_ + _).map(_._2)

    summed
  }
}
