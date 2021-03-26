package plugin.decimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.cloudera.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.AutoProgressMapper;
import org.apache.sqoop.mapreduce.MergeRecord;

import java.io.IOException;
import java.util.Map;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class MergeGenericRecordExportMapper<K, V> extends AutoProgressMapper<K, V, Text, MergeRecord> {

    protected MapWritable columnTypes = new MapWritable();
    private String keyColName;
    private boolean isNewDatasetSplit;

    public MergeGenericRecordExportMapper() {
    }

    protected void setup(Mapper<K, V, Text, MergeRecord>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.keyColName = conf.get("sqoop.merge.key.col");
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit)inputSplit;
        Path splitPath = fileSplit.getPath();
        if (splitPath.toString().startsWith(conf.get("sqoop.merge.new.path"))) {
            this.isNewDatasetSplit = true;
        } else {
            if (!splitPath.toString().startsWith(conf.get("sqoop.merge.old.path"))) {
                throw new IOException("File " + splitPath + " is not under new path " + conf.get("sqoop.merge.new.path") + " or old path " + conf.get("sqoop.merge.old.path"));
            }

            this.isNewDatasetSplit = false;
        }

        super.setup(context);
    }

    protected void processRecord(SqoopRecord sqoopRecord, Mapper<K, V, Text, MergeRecord>.Context context) throws IOException, InterruptedException {
        MergeRecord mergeRecord = new MergeRecord(sqoopRecord, this.isNewDatasetSplit);
        Map<String, Object> fieldMap = sqoopRecord.getFieldMap();
        if (null == fieldMap) {
            throw new IOException("No field map in record " + sqoopRecord);
        } else {
            Object keyObj = fieldMap.get(this.keyColName);
            if (null == keyObj) {
                throw new IOException("Cannot join values on null key. Did you specify a key column that exists?");
            } else {
                context.write(new Text(keyObj.toString()), mergeRecord);
            }
        }
    }
}