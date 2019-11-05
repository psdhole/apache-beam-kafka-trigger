/*
 *
 */
package com.demo.apachebeam.pipeline.triggers;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Class {@link PipelineProcessLogs}. */
public class PipelineProcessLogs {

  /** The Constant LOGGER. */
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineProcessLogs.class);

  /** The Constant BOOTSTRAP_SERVERS. */
  private static final String BOOTSTRAP_SERVERS = "broker:9092";

  /** The Constant INPUT_TOPIC. */
  private static final String INPUT_TOPIC = "input-log-topic";

  /** The Constant INPUT_TOPIC. */
  private static final String OUTPUT_FILE_NAME =
      "D:\\work\\gitrepo\\apache-beam-kafka-trigger-demo\\output\\LogMessages.csv";

  /** Recieve and send data. */
  private static void recieveAndSendData() {
    final PipelineOptions options =
        PipelineOptionsFactory.fromArgs().withValidation().as(PipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    PCollection<String> output =
        pipeline
            .apply(
                KafkaIO.<String, String>read()
                    .withBootstrapServers(BOOTSTRAP_SERVERS)
                    .withTopic(INPUT_TOPIC)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .updateConsumerProperties(
                        ImmutableMap.of("auto.offset.reset", (Object) "earliest"))
                    .withoutMetadata())
            .apply(
                "Apply Fixed window: ",
                Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply(
                MapElements.via(
                    new SimpleFunction<KV<String, String>, String>() {
                      private static final long serialVersionUID = 1L;

                      @Override
                      public String apply(KV<String, String> inputJSON) {
                        return inputJSON.getValue();
                      }
                    }));
    output.apply(TextIO.write().withWindowedWrites().to(OUTPUT_FILE_NAME).withNumShards(10));
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
