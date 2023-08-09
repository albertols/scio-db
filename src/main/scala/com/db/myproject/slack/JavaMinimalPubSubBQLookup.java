package com.db.myproject.slack;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * --project=your_project
 * --runner=DirectRunner
 * --tempLocation=gs://db-dev-europe-west3-gcs-125479-2-temp
 * --bqTable=your_project.CDH_dataset.scio_test
 * --pubSubTopic=projects/your_project/subscriptions/your_topic
 * --windowLengthPubSub=10
 * --windowLengthSideInput=86400
 * --windowStrategySideInput=default
 * --windowStrategyPubSub=fixed
 */
public class JavaMinimalPubSubBQLookup {
    private static final Logger LOG = LoggerFactory.getLogger(JavaMinimalPubSubBQLookup.class);

    public interface MyOptions extends PipelineOptions {
        @Description("Temporary location for storing files")
        String getTempLocation();

        @Description("BigQuery table specification")
        String getBqTable();
        void setBqTable(String value);
        @Description("PubSub topic")
        String getPubSubTopic();
        void setPubSubTopic(String value);

        @Description("Window length for PubSub in seconds")
        Long getWindowLengthPubSub();
        void setWindowLengthPubSub(Long value);

        @Description("Window length for Side Input in seconds")
        Long getWindowLengthSideInput();
        void setWindowLengthSideInput(Long value);
        @Description("Window strategy for Side Input: default, global, fixed")
        String getWindowStrategySideInput();

        void setWindowStrategySideInput(String value);

        @Description("Window strategy for PubSub: default, global, fixed")
        String getWindowStrategyPubSub();
        void setWindowStrategyPubSub(String value);
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        // reading options
        String bqTable = options.getBqTable();
        String pubSubTopic = options.getPubSubTopic();
        String windowStrategySideInput = options.getWindowStrategySideInput();
        Long windowLengthSideInput = options.getWindowLengthSideInput();
        Long windowLengthPubSub = options.getWindowLengthPubSub();
        String windowStrategyPubSub = options.getWindowStrategyPubSub();

        // sideInput
        PCollectionView<Map<String, String>> bigQuerySideInput = pipeline
                .apply("ReadBigQueryLookup", BigQueryIO.readTableRows()
                        .withTemplateCompatibility()
                        .from(bqTable))
                .apply("ConvertBigQueryRowsToMap", ParDo.of(new DoFn<TableRow, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        TableRow row = c.element();
                        String key = row.get("key").toString();
                        // Use a ValueProvider to provide a default value for opt
                        ValueProvider<String> optProvider = ValueProvider.StaticValueProvider.of(row.get("opt") != null ? row.get("opt").toString() : "");
                        LOG.info("Read BQ lookup: MyTableRow({},{})", key, optProvider);
                        c.output(KV.of(key, optProvider.get()));
                    }
                }))
                .apply("ApplyWindowing BigQuery", getCustomWindow(windowStrategySideInput, Duration.standardSeconds(windowLengthSideInput)))
                .apply("SideInputAsMap", View.asMap());

        // pubSub
        PCollection<KV<String, String>> pubsubRecords = pipeline
                .apply("ReadPubSub", PubsubIO.readStrings().fromSubscription(pubSubTopic))
                .apply("ParsePubSubRecords", ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String record = c.element(); // Assuming the PubSub record as a String
                        LOG.info("ParsePubSubRecord: {}", record);
                        String[] parts = record.split(",");
                        String key;
                        String value;
                        try {
                            key = parts[0];
                            value = parts[1];
                        } catch (Exception e) {
                            key ="ToxicRecord-"+ RandomStringUtils.randomAlphabetic(10);
                            value = String.format("%s",parts);
                        }
                        c.output(KV.of(key, value));
                    }
                }))
                .apply("ApplyWindowing PubSub", getCustomWindow(windowStrategyPubSub, Duration.standardSeconds(windowLengthPubSub)));

        PCollection<KV<String, String>> enrichedRecords = pubsubRecords
                .apply("EnrichWithBigQueryData", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String key = c.element().getKey();
                        String pubsubValue = c.element().getValue();
                        Map<String, String> bigQueryData = c.sideInput(bigQuerySideInput);
                        String bigQueryOptValue = bigQueryData.getOrDefault(key, "");
                        LOG.info("Joining key={}, pubsubValue={}, bigQueryOptValue={}", key, pubsubValue, bigQueryOptValue);
                        c.output(KV.of(key, pubsubValue + "," + bigQueryOptValue));
                    }
                }).withSideInputs(bigQuerySideInput));

        enrichedRecords.apply("PrintEnrichedRecords", ParDo.of(new DoFn<KV<String, String>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                LOG.info("Enriched Record: {}", c.element());
            }
        }));

        pipeline.run();
    }

    public static <T> Window<T> getCustomWindow(String winStrategy, Duration fixedWindowDuration) {
        Window<T> windowing;
        if ("fixed".equals(winStrategy)) {
            windowing = Window.<T>into(FixedWindows.of(fixedWindowDuration))
                    .triggering(Repeatedly.forever(AfterFirst.of(
                            AfterPane.elementCountAtLeast(1),
                            AfterProcessingTime.pastFirstElementInPane())))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes();
        } else if ("global".equals(winStrategy)) {
            windowing = Window.<T>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterFirst.of(
                            AfterPane.elementCountAtLeast(1),
                            AfterProcessingTime.pastFirstElementInPane())))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes();
        } else {
            windowing = Window.<T>into(new GlobalWindows());
        }
        return windowing;
    }

    class MyTableRow {
        String key;
        Optional<String> opt;

        public MyTableRow(String key, String value) {
        } // Empty constructor for serialization
        // Add getters, setters, and constructor as needed
    }
}

