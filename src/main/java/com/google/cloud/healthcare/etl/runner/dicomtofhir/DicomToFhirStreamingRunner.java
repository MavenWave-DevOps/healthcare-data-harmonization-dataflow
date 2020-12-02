package com.google.cloud.healthcare.etl.runner.dicomtofhir;

import com.google.cloud.healthcare.etl.model.ErrorEntry;
import com.google.cloud.healthcare.etl.model.converter.ErrorEntryConverter;
import com.google.cloud.healthcare.etl.model.mapping.HclsApiDicomMappableMessage;
import com.google.cloud.healthcare.etl.model.mapping.HclsApiDicomMappableMessageCoder;
import com.google.cloud.healthcare.etl.pipeline.MappingFn;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.healthcare.DicomIO;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO;
import org.apache.beam.sdk.io.gcp.healthcare.HealthcareIOError;
import org.apache.beam.sdk.io.gcp.healthcare.HealthcareIOErrorToTableRow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 * The entry point of the pipeline. It will be triggered upon receiving a PubSub message from the given subscription. From
 * the message, it will extract the webpath  of a DICOM instance and collect it's metadata from DICOM IO. With the
 * response, it will map the study metadata to a FHIR ImagingStudyResource and upload it to the given FHIR store.
 *
 * The errors for each component are handled separately, e.g. you can specify file paths for each
 * of the stage (read - DICOM IO, mapping, write - FHIR IO). Right now the shard is set to 1, if
 * you are seeing issues with regard to writing to GCS, feel free to bump it up to a reasonable
 * value.
 *
 */
public class DicomToFhirStreamingRunner {
    private static final Logger LOG = LoggerFactory.getLogger(DicomToFhirStreamingRunner.class);
    private static Duration ERROR_LOG_WINDOW_SIZE = Duration.standardSeconds(5);

    public interface Options extends PipelineOptions {
        @Description("The PubSub subscription to listen to, must be of the full format: projects/project_id/subscriptions/subscription_id")
        @Required
        String getPubSubSubscription();

        void setPubSubSubscription(String param1String);

        @Description("The path to the mapping configurations. The path will be treated as a GCS path if the path starts with the GCS scheme (\"gs\"), otherwise a local file. Please see: https://github.com/GoogleCloudPlatform/healthcare-data-harmonization/blob/baa4e0c7849413f7b44505a8410ee7f52745427a/mapping_configs/README.md for more details on the mapping configuration structure.")
        @Required
        String getMappingPath();

        void setMappingPath(String param1String);

        @Description("The target FHIR Store to write data to, must be of the full format: projects/project_id/locations/location/datasets/dataset_id/fhirStores/fhir_store_id")
        @Required
        String getFhirStore();

        void setFhirStore(String param1String);

        @Description("The path that is used to record all read errors. The path will be treated "
                + "as a GCS path if the path starts with the GCS scheme (\"gs\"), otherwise a local file.")
        @Required
        String getReadErrorPath();
        void setReadErrorPath(String readErrorPath);

        @Description("The path that is used to record all write errors. The path will be "
                + "treated as a GCS path if the path starts with the GCS scheme (\"gs\"), otherwise a "
                + "local file.")
        @Required
        String getWriteErrorPath();
        void setWriteErrorPath(String writeErrorPath);

        @Description("The path that is used to record all mapping errors. The path will be "
                + "treated as a GCS path if the path starts with the GCS scheme (\"gs\"), otherwise a "
                + "local file.")
        @Required
        String getMappingErrorPath();
        void setMappingErrorPath(String mappingErrorPath);

        @Description("The number of shards when writing errors to GCS.")
        @Default.Integer(10)
        Integer getErrorLogShardNum();
        void setErrorLogShardNum(Integer shardNum);
    }

    /**
    * A DoFn to collect the webpath of the DICOM instance from the PubSub Message.
     */
    public static class ExtractWebpathFromPubsub extends DoFn<PubsubMessage, String> {
        @ProcessElement
        public void processElement(DoFn<PubsubMessage, String>.ProcessContext context) throws UnsupportedEncodingException {
            PubsubMessage msg = context.element();
            String webpath = new String(msg.getPayload(), StandardCharsets.UTF_8);
            context.output(webpath);
            DicomToFhirStreamingRunner.LOG.info("Extracted webpath");
        }
    }

    /**
     * A DoFn that will take the response of the Study Metadata Read call from the DICOM API and reformat it into an
     * input to be consumed by the mapping library.
     */
    public static class CreateMappingFnInput extends DoFn<String, String> {
        @ProcessElement
        public void processElement(DoFn<String, String>.ProcessContext context) {
            String dicomMetadataResponse = context.element();
            Gson gson = new Gson();
            JsonArray jsonArray = gson.fromJson(dicomMetadataResponse, JsonArray.class);
            JsonObject jsonObject = new JsonObject();
            jsonObject.add("study", jsonArray);
            context.output(gson.toJson(jsonObject));
            DicomToFhirStreamingRunner.LOG.info("Sending instance to be mapped");
        }
    }

    /**
     * A DoFn that will take the response of the mapping library and wrap it into a FHIR bundle to be written to the
     * FHIR store.
     * TODO(b/174594428): Add a unique ID for each ImagingStudy FHIR resource to be uploaded to prevent creation of
     * multiple FHIR resources for each ImagingStudy.
     */
    public static class CreateFhirResourceBundle extends DoFn<String, String> {
        private static final String RequestMethod = "PUT";
        private static final String InnerResourceType = "ImagingStudy";

        @ProcessElement
        public void processElement(DoFn<String, String>.ProcessContext context) {
            String mappingOutputString = context.element();

            Gson gson = new Gson();
            JsonObject mappingOutput = gson.fromJson(mappingOutputString, JsonObject.class);
            JsonObject requestObj = new JsonObject();

            requestObj.addProperty("method", RequestMethod);
            requestObj.addProperty("url", InnerResourceType);
            JsonObject entryObj = new JsonObject();
            entryObj.add("resource", mappingOutput);
            entryObj.add("request", requestObj);
            JsonArray entries = new JsonArray();
            entries.add(entryObj);
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("resourceType", "Bundle");
            jsonObject.addProperty("type", "batch");
            jsonObject.add("entry", entries);
            context.output(gson.toJson(jsonObject));
            DicomToFhirStreamingRunner.LOG.info("Uploading to FHIR store");
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        MappingFn<HclsApiDicomMappableMessage> mappingFn = MappingFn.of(options.getMappingPath());

        DicomIO.ReadStudyMetadata.Result dicomResult = p
                .apply(PubsubIO.readMessages().fromSubscription(options.getPubSubSubscription()))
                .apply(ParDo.of(new ExtractWebpathFromPubsub()))
                .apply(DicomIO.readStudyMetadata());

        PCollection<String> successfulReads = dicomResult.getReadResponse();
        PCollection<String> failedReads = dicomResult.getFailedReads();

        HealthcareIOErrorToTableRow<String> errorConverter = new HealthcareIOErrorToTableRow<>();
        failedReads
                .apply(Window.<String>into(FixedWindows.of(ERROR_LOG_WINDOW_SIZE))
                        .triggering(
                                Repeatedly.forever(
                                        AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(ERROR_LOG_WINDOW_SIZE)))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply(
                        "WriteReadErrors",
                        TextIO.write().to(options.getReadErrorPath())
                                .withWindowedWrites()
                                .withNumShards(options.getErrorLogShardNum()));

        PCollectionTuple mapDicomStudyToFhirBundleRequest = successfulReads
                .apply(ParDo.of(new CreateMappingFnInput()))
                .apply(MapElements.into(
                        TypeDescriptor.of(HclsApiDicomMappableMessage.class))
                        .via(HclsApiDicomMappableMessage::from))
                .setCoder(HclsApiDicomMappableMessageCoder.of())
                .apply("MapMessages", ParDo.of(mappingFn)
                        .withOutputTags(MappingFn.MAPPING_TAG, TupleTagList.of(ErrorEntry.ERROR_ENTRY_TAG)));

        PCollection<ErrorEntry> mappingError = mapDicomStudyToFhirBundleRequest.get(ErrorEntry.ERROR_ENTRY_TAG);
        mappingError.apply("SerializeMappingErrors", MapElements.into(TypeDescriptors.strings())
                .via(e -> ErrorEntryConverter.toTableRow(e).toString()))
                .apply(Window.<String>into(FixedWindows.of(ERROR_LOG_WINDOW_SIZE))
                        .triggering(
                                Repeatedly.forever(
                                        AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(ERROR_LOG_WINDOW_SIZE)))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply("ReportMappingErrors",
                        TextIO.write().to(options.getMappingErrorPath())
                                .withWindowedWrites()
                                .withNumShards(options.getErrorLogShardNum()));

        FhirIO.Write.Result writeResults = mapDicomStudyToFhirBundleRequest
                .get(MappingFn.MAPPING_TAG)
                .apply(ParDo.of(new CreateFhirResourceBundle()))
                .apply("WriteFHIRBundles", FhirIO.Write.executeBundles(options.getFhirStore()));

        PCollection<HealthcareIOError<String>> failedWrites = writeResults.getFailedBodies();

        HealthcareIOErrorToTableRow<String> bundleErrorConverter =
                new HealthcareIOErrorToTableRow<>();
        failedWrites
                .apply("ConvertBundleErrors", MapElements.into(TypeDescriptors.strings())
                        .via(resp -> bundleErrorConverter.apply(resp).toString()))
                .apply(Window.<String>into(FixedWindows.of(ERROR_LOG_WINDOW_SIZE))
                        .triggering(
                                Repeatedly.forever(
                                        AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(ERROR_LOG_WINDOW_SIZE)))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply("RecordWriteErrors", TextIO.write().to(options.getWriteErrorPath())
                        .withWindowedWrites()
                        .withNumShards(options.getErrorLogShardNum()));

        p.run();
    }
}
