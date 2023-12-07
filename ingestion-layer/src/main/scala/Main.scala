import requests._
import upickle.default._

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration._

object HelloWorld extends App {
  given [A >: Null](using Decoder[A], Encoder[A]): Serde[A] with {
    def serialize(a: A): Array[Byte] =
      a.asJson.noSpaces.getBytes

    def deserialize(aAsBytes: Array[Byte]): Option[A] = {
      val aAsString = new String(aAsBytes)
      val aOrError = parser.decode[A](aAsString)
      aOrError match {
        case Right(a) => Some(a)
        case Left(error) =>
          println(s"There was an error converting the message $aOrError, $error")
          None
      }
    }
  }

  // get live matches
  var request = requests
    .get(
      url="https://footapi7.p.rapidapi.com/api/matches/live", 
        headers = Map(
          "X-RapidAPI-Key" -> "bfb32c3e32msh43fbdd4349980ffp1223e4jsned423ce098c3", 
          "X-RapidAPI-Host" -> "footapi7.p.rapidapi.com"
        )
    )
  println(ujson.read(request.text()))

  // get respective team's performance in its domestic league
  val team_id = 60
  val season_id = 41886
  val tournament_id = 17
  var url = s"https://footapi7.p.rapidapi.com/api/tournament/${tournament_id}/season/${season_id}/team/${team_id}/performance"
  request = requests
    .get(
      url=url, 
        headers = Map(
          "X-RapidAPI-Key" -> "bfb32c3e32msh43fbdd4349980ffp1223e4jsned423ce098c3", 
          "X-RapidAPI-Host" -> "footapi7.p.rapidapi.com"
        )
    )
  println(ujson.read(request.text()))
}

trait Serde[A] {
  def serialize(a: A): Array[Byte]
  def deserialize(aAsBytes: Array[Byte]): Option[A]
}
