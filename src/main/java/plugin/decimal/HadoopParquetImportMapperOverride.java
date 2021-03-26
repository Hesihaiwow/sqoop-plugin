package plugin.decimal;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;

import java.io.IOException;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class HadoopParquetImportMapperOverride extends ParquetImportMapperOverride<NullWritable, GenericRecord>{

    private static final Log LOG = LogFactory.getLog(HadoopParquetImportMapperOverride.class.getName());
    private static final String HADOOP_PARQUET_AVRO_SCHEMA_KEY = "parquetjob.avro.schema";

    public HadoopParquetImportMapperOverride() {
    }

    protected com.cloudera.sqoop.lib.LargeObjectLoader createLobLoader(Context context) throws IOException, InterruptedException {
        return new LargeObjectLoader(context.getConfiguration(), FileOutputFormat.getWorkOutputPath(context));
    }

    protected Schema getAvroSchema(Configuration configuration) {
        String schemaString = configuration.get("parquetjob.avro.schema");
        LOG.debug("Found Avro schema: " + schemaString);
        return AvroUtilOverride.parseAvroSchema(schemaString);
    }

    protected void write(Mapper<LongWritable, SqoopRecord, NullWritable, GenericRecord>.Context context, GenericRecord record) throws IOException, InterruptedException {
        context.write((NullWritable) null, record);
    }
}