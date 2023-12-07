import org.apache.kafka.clients.producer._
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration.durationInt
import zio.json._
import zio.kafka.consumer._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.stream._
import io.circe.{Codec, Decoder, Encoder}
import io.circe.derivation._

import java.util.UUID
import scala.util.{Failure, Success}

object Football extends zio.App {
    case class Player(name: String, score: Int) {
        override def toString: String = s"$name: $score"
    }

    object Player {
        given Encoder[Player] = deriveEncoder[Player]
        given Decoder[Player] = deriveDecoder[Player]
        given Codec[Player] = Codec.from(deriveDecoder, deriveEncoder)
    }

    case class Match(players: Array[Player]) {
        def score: String = s"${players(0)} - ${players(1)}"
    }

    object Match {
        given Encoder[Match] = deriveEncoder[Match]
        given Decoder[Match] = deriveDecoder[Match]
        given Codec[Match] = Codec.from(deriveDecoder, deriveEncoder)
    }
    
    val matchSerde: Serde[Any, Match] = Serde.string.inmapM { matchAsString =>
        ZIO.fromEither(matchAsString.fromJson[Match].left.map(new RuntimeException(_)))
    } { matchAsObj =>
        ZIO.effect(matchAsObj.toJson)
    }

    val consumerSettings: ConsumerSettings = ConsumerSettings(List("localhost:9092")).withGroupId("updates-consumer")

    val managedConsumer: RManaged[Clock with Blocking, Consumer.Service] = Consumer.make(consumerSettings)

    val consumer: ZLayer[Clock with Blocking, Throwable, Consumer] = Zlayer.fromManaged(managedConsumer)

    val matchesStreams: ZIO[Console with Any with Consumer with Clock, Throwable, Unit] = 
        Consumer.subscribeAnd(Subscription.topics("updates"))
        .plainStream(Serde.uuid, matchSerde.asTry)
        .map(cr => (cr.value, cr.offset))
        .tap { case (tryMatch, _) => 
            tryMatch match {
                case Success(matchz) => console.putStrLn(s"| ${matchz.score} |")
                case Failure(ex) => console.putStrLn(s"Poison pill ${ex.getMessage}")
            }
        }
        .map { case (_, offset) => offset }
        .aggregateAsync(Consumer.offsetBatches)
        .run(ZSink.foreach(_.commit))
    
    val producerSettings: ProducerSettings = ProducerSettings(List("localhost:9092"))

    val producer: ZLayer[Blocking, Throwable, Producer[Any, UUID, Match]] = 
        Zlayer.fromManaged(Producer.make[Any, UUID, Match](producerSettings, Serde.uuid, matchSerde))

    val testScore: Match = Match(Array(Player("Italy", 3), Player("England", 2)))
    val messagesToSend: ProducerRecord[UUID, Match] = 
        new ProducerRecord(
            "updates",
            UUID.fromString("bb3e8d1f-956f-4ff4-99a7-57d326d657b1"),
            testScore
        )

    val producerEffect: RIO[Producer[Any, UUID, Match], RecordMetadata] = 
        Producer.produce[Any, UUID, Match](messagesToSend)

    override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
        val program = for {
            _ <- matchesStreams.provideSomeLayer(consumer ++ Console.live).fork
            _ <- producerEffect.provideSomeLayer(producer) *> ZIO.sleep(5.seconds)
        } yield()
        program.exitCode
    }
}