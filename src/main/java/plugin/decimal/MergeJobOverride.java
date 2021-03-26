package plugin.decimal;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import com.cloudera.sqoop.SqoopOptions;
import org.apache.sqoop.avro.AvroUtil;
import com.cloudera.sqoop.manager.ConnManager;
import org.apache.sqoop.mapreduce.*;
import org.apache.sqoop.util.FileSystemUtil;
import org.apache.sqoop.util.Jars;

import java.io.IOException;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class MergeJobOverride extends JobBase {

    public static final String MERGE_OLD_PATH_KEY = "sqoop.merge.old.path";
    public static final String MERGE_NEW_PATH_KEY = "sqoop.merge.new.path";
    public static final String MERGE_KEY_COL_KEY = "sqoop.merge.key.col";
    public static final String MERGE_SQOOP_RECORD_KEY = "sqoop.merge.class";
    private final HadoopParquetMergeJobConfiguratorOverride parquetMergeJobConfigurator;

    public MergeJobOverride(SqoopOptions opts, HadoopParquetMergeJobConfiguratorOverride parquetMergeJobConfigurator) {
        super(opts, (Class)null, (Class)null, (Class)null);
        this.parquetMergeJobConfigurator = parquetMergeJobConfigurator;
    }

    public boolean runMergeJob() throws IOException {
        Configuration conf = this.options.getConf();
        Job job = this.createJob(conf);
        String userClassName = this.options.getClassName();
        if (null == userClassName) {
            throw new IOException("Record class name not specified with --class-name.");
        } else {
            String existingJar = this.options.getExistingJarName();
            if (existingJar != null) {
                LOG.debug("Setting job jar to user-specified jar: " + existingJar);
                job.getConfiguration().set("mapred.jar", existingJar);
            } else {
                try {
                    Class<? extends Object> userClass = conf.getClassByName(userClassName);
                    if (null != userClass) {
                        String userJar = Jars.getJarPathForClass(userClass);
                        LOG.debug("Setting job jar based on user class " + userClassName + ": " + userJar);
                        job.getConfiguration().set("mapred.jar", userJar);
                    } else {
                        LOG.warn("Specified class " + userClassName + " is not in a jar. MapReduce may not find the class");
                    }
                } catch (ClassNotFoundException var12) {
                    throw new IOException(var12);
                }
            }

            try {
                Path oldPath = new Path(this.options.getMergeOldPath());
                Path newPath = new Path(this.options.getMergeNewPath());
                Configuration jobConf = job.getConfiguration();
                oldPath = FileSystemUtil.makeQualified(oldPath, jobConf);
                newPath = FileSystemUtil.makeQualified(newPath, jobConf);
                this.propagateOptionsToJob(job);
                FileInputFormat.addInputPath(job, oldPath);
                FileInputFormat.addInputPath(job, newPath);
                jobConf.set("sqoop.merge.old.path", oldPath.toString());
                jobConf.set("sqoop.merge.new.path", newPath.toString());
                jobConf.set("sqoop.merge.key.col", this.options.getMergeKeyCol());
                jobConf.set("sqoop.merge.class", userClassName);
                FileOutputFormat.setOutputPath(job, new Path(this.options.getTargetDir()));
                ExportJobBase.FileType fileType = ExportJobBase.getFileType(jobConf, oldPath);
                switch(fileType) {
                    case PARQUET_FILE:
                        Path finalPath = new Path(this.options.getTargetDir());
                        finalPath = FileSystemUtil.makeQualified(finalPath, jobConf);
                        this.parquetMergeJobConfigurator.configureParquetMergeJob(jobConf, job, oldPath, newPath, finalPath);
                        break;
                    case AVRO_DATA_FILE:
                        this.configueAvroMergeJob(conf, job, oldPath, newPath);
                        break;
                    case SEQUENCE_FILE:
                        job.setInputFormatClass(SequenceFileInputFormat.class);
                        job.setOutputFormatClass(SequenceFileOutputFormat.class);
                        job.setMapperClass(MergeRecordMapper.class);
                        job.setReducerClass(MergeReducer.class);
                        break;
                    default:
                        job.setMapperClass(MergeTextMapper.class);
                        job.setOutputFormatClass(RawKeyTextOutputFormat.class);
                        job.setReducerClass(MergeReducer.class);
                }

                jobConf.set("mapred.output.key.class", userClassName);
                job.setOutputValueClass(NullWritable.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(MergeRecord.class);
                this.cacheJars(job, (ConnManager)null);
                this.setJob(job);
                return this.runJob(job);
            } catch (InterruptedException var10) {
                throw new IOException(var10);
            } catch (ClassNotFoundException var11) {
                throw new IOException(var11);
            }
        }
    }

    private void configueAvroMergeJob(Configuration conf, Job job, Path oldPath, Path newPath) throws IOException {
        LOG.info("Trying to merge avro files");
        Schema oldPathSchema = AvroUtil.getAvroSchema(oldPath, conf);
        Schema newPathSchema = AvroUtil.getAvroSchema(newPath, conf);
        if (oldPathSchema != null && newPathSchema != null && oldPathSchema.equals(newPathSchema)) {
            LOG.debug("Avro Schema:" + oldPathSchema);
            job.setInputFormatClass(AvroInputFormat.class);
            job.setOutputFormatClass(AvroOutputFormat.class);
            job.setMapperClass(MergeAvroMapper.class);
            job.setReducerClass(MergeAvroReducer.class);
            AvroJob.setOutputSchema(job.getConfiguration(), oldPathSchema);
        } else {
            throw new IOException("Invalid schema for input directories. Schema for old data: [" + oldPathSchema + "]. Schema for new data: [" + newPathSchema + "]");
        }
    }
}