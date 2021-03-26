package plugin.decimal;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.GenericRecordExportMapper;

import java.io.IOException;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class HadoopParquetExportMapperOverride extends GenericRecordExportMapper<Void, GenericRecord> {

    public HadoopParquetExportMapperOverride() {
    }

    protected void map(Void key, GenericRecord val, Mapper<Void, GenericRecord, SqoopRecord, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(this.toSqoopRecord(val), NullWritable.get());
    }
}