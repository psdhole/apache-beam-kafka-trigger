/*
 *
 */
package com.demo.apachebeam.pipeline.triggers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** The Class {@link PipelineProcessLogs}. */
public class PipelineProcessLogs {

  /** The Constant LOGGER. */
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineProcessLogs.class);

  /** The Constant BOOTSTRAP_SERVERS. */
  private static final String BOOTSTRAP_SERVERS = "broker:9092";

  /** The Constant INPUT_TOPIC. */
  private static final String INPUT_TOPIC = "input-log-topic";

  /** The Constant INPUT_TOPIC. */
  private static final String OUTPUT_FILE_NAME = "output";

  /** Recieve and send data. */
  private static void recieveAndSendData() {
    final PipelineOptions options =
        PipelineOptionsFactory.fromArgs().withValidation().as(PipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            KafkaIO.<String, String>read()
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .withTopic(INPUT_TOPIC)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object) "earliest"))
                .withoutMetadata())
        .apply(
            "Apply Fixed window: ",
            Window.<KV<String, String>>into(FixedWindows.of(Duration.standardMinutes(2)))
                .triggering(
                    Repeatedly.forever(
                        AfterFirst.of(
                            AfterPane.elementCountAtLeast(90),
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardMinutes(2)))))
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
        .apply(
            MapElements.via(
                new SimpleFunction<KV<String, String>, String>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  public String apply(KV<String, String> inputJSON) {
                    String csvRow = "";
                    try {
                      ObjectMapper mapper = new ObjectMapper();
                      Map<String, String> map = mapper.readValue(inputJSON.getValue(), Map.class);
                      String logType = (String) map.get("logType");
                      String logSeverity = (String) map.get("logSeverity");
                      String logPriority = (String) map.get("logPriority");
                      String logDescription = (String) map.get("logDescription");
                      csvRow = String.join(",", logType, logPriority, logSeverity, logDescription);
                    } catch (Exception e) {
                      LOGGER.debug("Error while parsing JSON :", e);
                    }
                    return csvRow;
                  }
                }))
        .apply(
            TextIO.write()
                .withWindowedWrites()
                .withShardNameTemplate("-logfile-SS-of-NN")
                .to(OUTPUT_FILE_NAME)
                .withNumShards(5)
                .withSuffix(".csv"));
    pipeline.run();
    LOGGER.debug("All done ..!!");
  }

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {
    recieveAndSendData();
  }
}
