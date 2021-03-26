package plugin.decimal;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.sqoop.avro.AvroUtil;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.MergeRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public abstract class MergeParquetReducer<KEYOUT, VALUEOUT> extends Reducer<Text, MergeRecord, KEYOUT, VALUEOUT> {

    private Schema schema = null;
    private boolean bigDecimalFormatString = true;
    private Map<String, Pair<String, String>> sqoopRecordFields = new HashMap();

    public MergeParquetReducer() {
    }

    protected void setup(Reducer<Text, MergeRecord, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        this.schema = (new Schema.Parser()).parse(context.getConfiguration().get("parquetjob.avro.schema"));
        this.bigDecimalFormatString = context.getConfiguration().getBoolean("sqoop.bigdecimal.format.string", true);
    }

    public void reduce(Text key, Iterable<MergeRecord> vals, Reducer<Text, MergeRecord, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        SqoopRecord bestRecord = null;

        try {
            Iterator var5 = vals.iterator();

            while(var5.hasNext()) {
                MergeRecord mergeRecord = (MergeRecord)var5.next();
                if (null == bestRecord && !mergeRecord.isNewRecord()) {
                    bestRecord = (SqoopRecord)mergeRecord.getSqoopRecord().clone();
                } else if (mergeRecord.isNewRecord()) {
                    bestRecord = (SqoopRecord)mergeRecord.getSqoopRecord().clone();
                }
            }
        } catch (CloneNotSupportedException var7) {
            throw new IOException(var7);
        }

        if (null != bestRecord) {
            GenericRecord record = AvroUtil.toGenericRecord(bestRecord.getFieldMap(), this.schema, this.bigDecimalFormatString);
            this.write(context, record);
        }

    }

    protected abstract void write(Reducer<Text, MergeRecord, KEYOUT, VALUEOUT>.Context var1, GenericRecord var2) throws IOException, InterruptedException;

}