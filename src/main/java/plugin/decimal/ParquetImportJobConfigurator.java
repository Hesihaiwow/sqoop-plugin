package plugin.decimal;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.SqoopOptions;

import java.io.IOException;

public interface ParquetImportJobConfigurator {

    void configureMapper(Job var1, Schema var2, SqoopOptions var3, String var4, Path var5) throws IOException;

    Class<? extends Mapper> getMapperClass();

    Class<? extends OutputFormat> getOutputFormatClass();

    boolean isHiveImportNeeded();
}
