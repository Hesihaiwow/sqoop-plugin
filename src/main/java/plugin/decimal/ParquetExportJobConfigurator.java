package plugin.decimal;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public interface ParquetExportJobConfigurator {

    void configureInputFormat(Job var1, Path var2) throws IOException;

    Class<? extends Mapper> getMapperClass();

    Class<? extends InputFormat> getInputFormatClass();
}
