package plugin.decimal;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.sqoop.avro.AvroUtil;

import java.io.IOException;
import java.util.Collections;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class HadoopParquetMergeJobConfiguratorOverride {

    public static final Log LOG = LogFactory.getLog(HadoopParquetMergeJobConfiguratorOverride.class.getName());
    private final HadoopParquetImportJobConfigurator importJobConfigurator;
    private final HadoopParquetExportJobConfigurator exportJobConfigurator;

    public HadoopParquetMergeJobConfiguratorOverride(HadoopParquetImportJobConfigurator importJobConfigurator, HadoopParquetExportJobConfigurator exportJobConfigurator) {
        this.importJobConfigurator = importJobConfigurator;
        this.exportJobConfigurator = exportJobConfigurator;
    }

    public HadoopParquetMergeJobConfiguratorOverride() {
        this(new HadoopParquetImportJobConfigurator(), new HadoopParquetExportJobConfigurator());
    }

    public void configureParquetMergeJob(Configuration conf, Job job, Path oldPath, Path newPath, Path finalPath) throws IOException {
        try {
            LOG.info("Trying to merge parquet files");
            job.setOutputKeyClass(Void.class);
            job.setMapperClass(MergeParquetMapperOverride.class);
            job.setReducerClass(MergeParquetReducerOverride.class);
            job.setOutputValueClass(GenericRecord.class);
            Schema avroSchema = this.loadAvroSchema(conf, oldPath);
            this.validateNewPathAvroSchema(AvroUtilOverride.getAvroSchemaFromParquetFile(newPath, conf), avroSchema);
            job.setInputFormatClass(this.exportJobConfigurator.getInputFormatClass());
            AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);
            conf.set("parquetjob.avro.schema", avroSchema.toString());
            this.importJobConfigurator.configureAvroSchema(job, avroSchema);
            this.importJobConfigurator.configureOutputCodec(job);
            job.setOutputFormatClass(this.importJobConfigurator.getOutputFormatClass());
        } catch (Exception var7) {
            throw new IOException(var7);
        }
    }

    private Schema loadAvroSchema(Configuration conf, Path path) throws IOException {
        Schema avroSchema = AvroUtilOverride.getAvroSchemaFromParquetFile(path, conf);
        if (avroSchema == null) {
            throw new RuntimeException("Could not load Avro schema from path: " + path);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Avro schema loaded: " + avroSchema);
            }

            return avroSchema;
        }
    }

    private void validateNewPathAvroSchema(Schema newPathAvroSchema, Schema avroSchema) {
        if (newPathAvroSchema != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Validation Avro schema %s against %s", newPathAvroSchema.toString(), avroSchema.toString()));
            }

            SchemaValidator schemaValidator = (new SchemaValidatorBuilder()).mutualReadStrategy().validateAll();

            try {
                schemaValidator.validate(newPathAvroSchema, Collections.singleton(avroSchema));
            } catch (SchemaValidationException var5) {
                throw new RuntimeException("Cannot merge files, the Avro schemas are not compatible.", var5);
            }
        }
    }
}