package plugin.decimal;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class HadoopParquetExportJobConfigurator {

    public HadoopParquetExportJobConfigurator() {
    }

    public void configureInputFormat(Job job, Path inputPath) throws IOException {
    }

    public Class<? extends Mapper> getMapperClass() {
        return HadoopParquetExportMapperOverride.class;
    }

    public Class<? extends InputFormat> getInputFormatClass() {
        return AvroParquetInputFormat.class;
    }
}