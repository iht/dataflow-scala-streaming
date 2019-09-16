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
    val (rides, writableErrors) = parseJSONStrings(messages)

    /*_*/
    rides.saveAsTypedBigQuery(goodTable, WRITE_APPEND, CREATE_IF_NEEDED)
    writableErrors.saveAsTypedBigQuery(badTable, WRITE_APPEND, CREATE_IF_NEEDED)
    /*_*/

    // Group by session with a max duration of 5 mins between events
    // Window options
    val wopts: WindowOptions = customWindowOptions
    val groupRides = groupRidesByKey(rides.map(_.toTaxiRide), wopts)
    /*_*/
    groupRides.saveAsTypedBigQuery(accumTable, WRITE_APPEND, CREATE_IF_NEEDED)
    /*_*/

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
    val pubsubInput: PubsubIO[String] = PubsubIO.apply[String](
      pubsubTopic,
      "ride_id",
      "timestamp")
    val pubsubParams: PubsubIO.ReadParam = PubsubIO.ReadParam(false)

    /*_*/
    // Disable type aware highlights for this code (false positive)
    val messages: SCollection[String] = sc.read(pubsubInput)(pubsubParams) /*_*/
      .withName("Read from PubSub")

    messages
  }

  def parseJSONStrings(messages: SCollection[String]):
    (SCollection[PointTaxiRide], SCollection[JsonError]) = {
    val objects: SCollection[Either[circe.Error, PointTaxiRide]] = messages.map(json2TaxiRide)
      .withName("Parse JSON")

    val (errorsEither, ridesEither) = objects.partition(_.isLeft)
    val errors: SCollection[circe.Error] = errorsEither.filter(_.isLeft).map(_.left.get)
      .withName("Get errors")
    val rides: SCollection[PointTaxiRide] = ridesEither.filter(_.isRight).map(_.right.get)
      .withName("Get taxi rides")

    val writableErrors: SCollection[JsonError] = errors.map(circeErrorToCustomError)
      .withName("Prepare errors for BQ")

    (rides, writableErrors)
  }

  def groupRidesByKey(rides: SCollection[TaxiRide], wopts: WindowOptions): SCollection[TaxiRide] = {
    val ridesByKey: SCollection[(String, TaxiRide)] = rides.keyBy(_.ride_id)
      .withSessionWindows(Duration.standardSeconds(SESSION_GAP), wopts)

    // Now "sum" to obtain aggregated values
    val summed: SCollection[TaxiRide] = ridesByKey.reduceByKey(_ + _).map(_._2)

    summed
  }
}
