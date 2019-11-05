/*
 *
 */
package com.demo.apachebeam.pipeline.triggers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** The Class {@link LogMessageProducer}. */
public class LogMessageProducer {

  /** The Constant LOGGER. */
  private static final Logger LOGGER = LoggerFactory.getLogger(LogMessageProducer.class);

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {
    for (long i = 0; i < 10; i++) {
      LogMessage message1 = new LogMessage(LogType.INFO.toString(), "1", "1", "msg111" + i);
      LogMessage message2 = new LogMessage(LogType.ERROR.toString(), "2", "2", "msg222" + i);
      LogMessage message3 = new LogMessage(LogType.TRACE.toString(), "3", "3", "msg333" + i);
      LogMessage message4 = new LogMessage(LogType.WARNING.toString(), "4", "4", "msg444" + i);
      LogMessage message5 = new LogMessage(LogType.FATAL.toString(), "5", "5", "msg555" + i);
      LogMessage message6 = new LogMessage(LogType.DEBUG.toString(), "5", "5", "msg666" + i);
      sendLog(message1);
      sendLog(message2);
      sendLog(message3);
      sendLog(message4);
      sendLog(message5);
      sendLog(message6);
    }
  }

  /**
   * Method to send log message on the kafka topic.
   *
   * @param message log message to send on kafka topic
   */
  private static void sendLog(LogMessage message) {
    String topic = "input-log-topic";
    Properties props = new Properties();
    props.put("bootstrap.servers", "broker:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    ObjectMapper mapper = new ObjectMapper();
    try (Producer<String, String> producer = new KafkaProducer<>(props) ) {
      String logMessage = mapper.writeValueAsString(message);
      ProducerRecord<String, String> logMessageRecordToSend =
          new ProducerRecord<>(topic, message.getLogType(), logMessage);
      Thread.sleep(5000);
      LOGGER.debug("Sending log message: {}", logMessage);
      producer.send(logMessageRecordToSend).get();
    } catch (Exception e) {
      LOGGER.error("error while sending message :", e);
    }
  }
}
