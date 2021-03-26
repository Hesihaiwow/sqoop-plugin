package plugin.decimal;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.GenericDataSupplier;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.sqoop.SqoopOptions;

import java.io.IOException;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class HadoopParquetImportJobConfigurator implements ParquetImportJobConfigurator {

    private static final Log LOG = LogFactory.getLog(HadoopParquetImportJobConfigurator.class.getName());

    public HadoopParquetImportJobConfigurator() {
    }

    public void configureMapper(Job job, Schema schema, SqoopOptions options, String tableName, Path destination) throws IOException {
        this.configureAvroSchema(job, schema);
        this.configureOutputCodec(job);
        this.configureLogicalTypeSupport(job, options);
    }

    private void configureLogicalTypeSupport(Job job, SqoopOptions options) {
        if (options.getConf().getBoolean("sqoop.parquet.logical_types.decimal.enable", false)) {
            AvroParquetOutputFormat.setAvroDataSupplier(job, GenericDataSupplier.class);
        }

    }

    public Class<? extends Mapper> getMapperClass() {
        return HadoopParquetImportMapperOverride.class;
    }

    public Class<? extends OutputFormat> getOutputFormatClass() {
        return AvroParquetOutputFormat.class;
    }

    public boolean isHiveImportNeeded() {
        return true;
    }

    void configureOutputCodec(Job job) {
        String outputCodec = job.getConfiguration().get("parquetjob.output.codec");
        if (outputCodec != null) {
            LOG.info("Using output codec: " + outputCodec);
            ParquetOutputFormat.setCompression(job, CompressionCodecName.fromConf(outputCodec));
        }

    }

    void configureAvroSchema(Job job, Schema schema) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Using Avro schema: " + schema);
        }

        AvroParquetOutputFormat.setSchema(job, schema);
    }
}