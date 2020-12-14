package test

import cats.effect.{ExitCode, Resource}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import monix.eval.{Task, TaskApp}
import monix.kafka.config.AutoOffsetReset
import monix.kafka.{CommittableOffset, CommittableOffsetBatch, KafkaConsumerConfig, KafkaConsumerObservable}
import monix.reactive.Observable
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.concurrent.duration._

object TestApp extends TaskApp {
  case class TestState(
      elements: List[String] = List.empty,
      committableBatch: CommittableOffsetBatch = CommittableOffsetBatch.empty
  ) {
    def addNextElement(value: String, offset: CommittableOffset): TestState =
      TestState(value :: elements, committableBatch.updated(offset))
  }

  override def run(args: List[String]): Task[ExitCode] = {
    val kafkaPort = 19092
    val topicName = "test_topic"
    val bootstrapServer = s"localhost:$kafkaPort"
    implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(kafkaPort = kafkaPort, zooKeeperPort = 12181)
    Resource.make(Task(EmbeddedKafka.start()))(embedded => Task(embedded.stop(true))).use { _ =>
      for {
        logger <- Slf4jLogger.fromClass[Task](getClass)
        _ <- createProducer(bootstrapServer).use { producer =>
          Task(producer.send(new ProducerRecord(topicName, "message1"))) >>
            Task(producer.send(new ProducerRecord(topicName, "message2")))
        }
        _ <- logger.info("starting logic")
        _ <- logic(bootstrapServer, topicName)
      } yield ExitCode.Success
    }
  }

  private def logic(bootstrapServer: String, topic: String) = {
    val kafkaConfig: KafkaConsumerConfig = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(bootstrapServer),
      groupId = "failing-logic",
      autoOffsetReset = AutoOffsetReset.Earliest
    )
    KafkaConsumerObservable
      .manualCommit[String, String](kafkaConfig, List(topic))
      .timeoutOnSlowUpstreamTo(5.seconds, Observable.empty)
      .foldLeft(CommittableOffsetBatch.empty) { case (batch, message) => batch.updated(message.committableOffset) }
      .mapEval(completeBatch => completeBatch.commitAsync())
      .headOrElseL(List.empty)
  }

  private def createProducer(bootstrapServers: String): Resource[Task, KafkaProducer[String, String]] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    Resource.fromAutoCloseable[Task, KafkaProducer[String, String]](Task(new KafkaProducer(props)))
  }
}
