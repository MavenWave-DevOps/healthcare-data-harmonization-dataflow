// Copyright 2021 Google LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.cloud.healthcare.etl.runner.omoptofhir;

import com.google.cloud.WriteChannel;
import com.google.cloud.healthcare.etl.model.ErrorEntry;
import com.google.cloud.healthcare.etl.model.converter.ErrorEntryConverter;
import com.google.cloud.healthcare.etl.model.mapping.*;
import com.google.cloud.healthcare.etl.pipeline.MappingFn;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO;
import org.apache.beam.sdk.io.gcp.healthcare.HealthcareIOError;
import org.apache.beam.sdk.io.gcp.healthcare.HealthcareIOErrorToTableRow;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * The entry point of the pipeline. It will read the OMOP data from GCS bucket. With the GCS file data,
 * it will map to a FHIR and upload it to the given FHIR store.
 *
 * <p>The errors for each component are handled separately, e.g. you can specify file paths for each
 * stage (read - OMOP, mapping, write - FHIR IO).
 */
public class OmopToFhirBatchRunner {

    private static final Logger LOG = LoggerFactory.getLogger(OmopToFhirBatchRunner.class);

    private static Duration ERROR_LOG_WINDOW_SIZE = Duration.standardSeconds(5);

    /**
     * Pipeline options.
     */
    public interface Options extends PipelineOptions {

        @Description("The input file pattern to read from (e.g. gs://bucket-name/*.json)")
        @Required
        String getInputFilePattern();

        void setInputFilePattern(String inputFilePattern);

        @Description(
                "The path to the mapping configurations. The path will be treated as a GCS path if the"
                        + " path starts with the GCS scheme (\"gs\"), otherwise a local file. Please see: "
                        + "https://github.com/GoogleCloudPlatform/healthcare-data-harmonization/blob/baa4e0c7849413f7b44505a8410ee7f52745427a/mapping_configs/README.md"
                        + " for more details on the mapping configuration structure.")
        @Required
        String getMappingPath();

        void setMappingPath(String gcsPath);

        @Description("The output location to write to (e.g. gs://bucket-name/fhir/)")
        @Required
        String getOutputDirectory();

        void setOutputDirectory(String value);

        @Description(
                "The path that is used to record all read errors. The path will be treated as a GCS path"
                        + " if the path starts with the GCS scheme (\"gs\"), otherwise a local file.")
        @Required
        String getReadErrorPath();

        void setReadErrorPath(String readErrorPath);

        @Description(
                "The path that is used to record all write errors. The path will be "
                        + "treated as a GCS path if the path starts with the GCS scheme (\"gs\"), otherwise a "
                        + "local file.")
        @Required
        String getWriteErrorPath();

        void setWriteErrorPath(String writeErrorPath);

        @Description(
                "The path that is used to record all mapping errors. The path will be "
                        + "treated as a GCS path if the path starts with the GCS scheme (\"gs\"), otherwise a "
                        + "local file.")
        @Required
        String getMappingErrorPath();

        void setMappingErrorPath(String mappingErrorPath);

        @Description(
                "The target FHIR Store to write data to, must be of the full format: "
                        + "projects/project_id/locations/location/datasets/dataset_id/fhirStores/fhir_store_id")
        @Required
        String getFhirStore();

        void setFhirStore(String param1String);

        @Description("The path that is used to write temp files before import.")
        String getFhirImportGcsTempPath();

        void setFhirImportGcsTempPath(String fhirImportGcsTempPath);

        @Description("The path that is used to write dead/error files while import, which failed to import.")
        String getFhirImportGcsDeadLetterPath();

        void setFhirImportGcsDeadLetterPath(String fhirImportGcsDeadLetterPath);

        @Description("The number of shards when writing errors to GCS.")
        @Default.Integer(10)
        Integer getErrorLogShardNum();

        void setErrorLogShardNum(Integer shardNum);
    }

    /**
     * A DoFn that will take the GCS path and read OMOP data to be consumed by the mapping library.
     */
    static class CreateMappingFnInput extends DoFn<String, String> {

        @ProcessElement
        public void processElement(DoFn<String, String>.ProcessContext context) {
            Storage storage = StorageOptions.newBuilder().build().getService();
            String gcsBlobPath = context.element();
            String bucketName = getBucketName(gcsBlobPath);
            String objectName = getObjectName(gcsBlobPath);
            byte[] input = storage.readAllBytes(bucketName, objectName);
            String jsonContent = new String(input);
            context.output(jsonContent);
        }
    }

    static class WriteFnOutput extends DoFn<String, String> {
        private String outputDirectory;

        public WriteFnOutput(String outputDirectory) {
            this.outputDirectory = outputDirectory;
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            Storage storage = StorageOptions.newBuilder().build().getService();
            String json = context.element();
            String bucketName = getBucketName(outputDirectory);
            String folderPath = getObjectName(outputDirectory);

            BlobId blobId = BlobId.of(bucketName, folderPath + System.currentTimeMillis() + ".json");
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
            byte[] content = json.getBytes(StandardCharsets.UTF_8);
            WriteChannel writer = null;
            try {
                writer = storage.writer(blobInfo);
                writer.write(ByteBuffer.wrap(content, 0, content.length));
            } catch (Exception ex) {
            } finally {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            context.output(json);
        }
    }

    private static final String GCS_PATH_PREFIX = "gs://";

    private static String getBucketName(String gcsBlobPath) {
        if (!gcsBlobPath.startsWith(GCS_PATH_PREFIX)) {
            throw new IllegalArgumentException(
                    "GCS blob paths must start with gs://, got " + gcsBlobPath);
        }

        String bucketAndObjectName = gcsBlobPath.substring(GCS_PATH_PREFIX.length());
        int firstSlash = bucketAndObjectName.indexOf("/");
        if (firstSlash == -1) {
            throw new IllegalArgumentException(
                    "GCS blob paths must have format gs://my-bucket-name/my-object-name, got " + gcsBlobPath);
        }
        return bucketAndObjectName.substring(0, firstSlash);
    }

    private static String getObjectName(String gcsBlobPath) {
        if (!gcsBlobPath.startsWith(GCS_PATH_PREFIX)) {
            throw new IllegalArgumentException(
                    "GCS blob paths must start with gs://, got " + gcsBlobPath);
        }

        String bucketAndObjectName = gcsBlobPath.substring(GCS_PATH_PREFIX.length());
        int firstSlash = bucketAndObjectName.indexOf("/");
        if (firstSlash == -1) {
            throw new IllegalArgumentException(
                    "GCS blob paths must have format gs://my-bucket-name/my-object-name, got " + gcsBlobPath);
        }
        return bucketAndObjectName.substring(firstSlash + 1);
    }


    /**
     * Map the given OMOP Data to a FHIR resource.
     *
     * @param omopData A PCollection of Strings containing OMOP Data.
     * @param options  The pipeline configuration.
     * @return A PCollection of String containing successfully mapped FHIR resources.
     */
    private PCollection<String> mapOmopToFhirResource(
            PCollection<String> omopData, Options options) {

        MappingFn<GcsOmopMappableMessage> mappingFn =
                MappingFn.of(options.getMappingPath(), false);

        PCollectionTuple mapOmopToFhirBundleRequest =
                omopData
                        .apply(ParDo.of(new CreateMappingFnInput()))
                        .apply(
                                MapElements.into(TypeDescriptor.of(GcsOmopMappableMessage.class))
                                        .via(GcsOmopMappableMessage::from))
                        .setCoder(GcsOmopMappableMessageCoder.of())
                        .apply(
                                "MapMessages",
                                ParDo.of(mappingFn)
                                        .withOutputTags(
                                                MappingFn.MAPPING_TAG, TupleTagList.of(ErrorEntry.ERROR_ENTRY_TAG)));

        PCollection<ErrorEntry> mappingError =
                mapOmopToFhirBundleRequest.get(ErrorEntry.ERROR_ENTRY_TAG);
        mappingError
                .apply(
                        "SerializeMappingErrors",
                        MapElements.into(TypeDescriptors.strings())
                                .via(e -> ErrorEntryConverter.toTableRow(e).toString()))
                .apply(
                        Window.<String>into(FixedWindows.of(ERROR_LOG_WINDOW_SIZE))
                                .triggering(
                                        Repeatedly.forever(
                                                AfterProcessingTime.pastFirstElementInPane()
                                                        .plusDelayOf(ERROR_LOG_WINDOW_SIZE)))
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes())
                .apply(
                        "ReportMappingErrors",
                        TextIO.write()
                                .to(options.getMappingErrorPath())
                                .withWindowedWrites()
                                .withNumShards(options.getErrorLogShardNum()));

        return mapOmopToFhirBundleRequest
                .get(MappingFn.MAPPING_TAG)
                .setCoder(MappedFhirMessageWithSourceTimeCoder.of())
                .apply(MapElements.into(TypeDescriptors.strings()).via(MappingOutput::getOutput));
    }

    /**
     * Write the mapped FHIR resources to the given FHIR store.
     *
     * @param fhirResource A PCollection of String containing successfully mapped FHIR resources.
     * @param options      The pipeline configuration.
     */
    private void writeToFhirStore(PCollection<String> fhirResource, Options options) {
        FhirIO.Write.Result writeResults = null;
        if(options.getFhirImportGcsDeadLetterPath() != null && !options.getFhirImportGcsDeadLetterPath().trim().equals("")){
            writeResults =
                    fhirResource
                            .apply("ImportFHIRBundles", FhirIO.Write.fhirStoresImport(options.getFhirStore(), options.getFhirImportGcsTempPath(), options.getFhirImportGcsDeadLetterPath(), FhirIO.Import.ContentStructure.BUNDLE));
        } else {
            writeResults =
                    fhirResource
                            .apply("ExecuteFHIRBundles", FhirIO.Write.executeBundles(options.getFhirStore()));
        }

        PCollection<HealthcareIOError<String>> failedWrites = writeResults.getFailedBodies();

        HealthcareIOErrorToTableRow<String> bundleErrorConverter = new HealthcareIOErrorToTableRow<>();
        failedWrites
                .apply(
                        "ConvertBundleErrors",
                        MapElements.into(TypeDescriptors.strings())
                                .via(resp -> bundleErrorConverter.apply(resp).toString()))
                .apply(
                        Window.<String>into(FixedWindows.of(ERROR_LOG_WINDOW_SIZE))
                                .triggering(
                                        Repeatedly.forever(
                                                AfterProcessingTime.pastFirstElementInPane()
                                                        .plusDelayOf(ERROR_LOG_WINDOW_SIZE)))
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes())
                .apply(
                        "RecordWriteErrors",
                        TextIO.write()
                                .to(options.getWriteErrorPath())
                                .withWindowedWrites()
                                .withNumShards(options.getErrorLogShardNum()));
    }

    public static void main(String[] args) {
        OmopToFhirBatchRunner runner = new OmopToFhirBatchRunner();
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> omopData = pipeline
                .apply("MatchFile(s)", FileIO.match().filepattern(options.getInputFilePattern()))
                .apply("Reading matching files", FileIO.readMatches())
                .apply("Create PCollection",
                        MapElements.into(TypeDescriptors.strings())
                                .via(
                                        (FileIO.ReadableFile file) -> {
                                            String fileName = file.getMetadata().resourceId().toString();
                                            return fileName;
                                        }));

        PCollection<String> fhirResource =
                runner.mapOmopToFhirResource(omopData, options)
                        .apply("Write to Bucket", ParDo.of(new WriteFnOutput(options.getOutputDirectory())));
        runner.writeToFhirStore(fhirResource, options);

        pipeline.run();
    }
}
