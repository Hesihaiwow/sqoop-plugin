//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package plugin.decimal;

import java.io.IOException;
import java.sql.SQLException;
import org.apache.avro.Schema;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.AutoProgressMapper;

public abstract class ParquetImportMapperOverride<KEYOUT, VALOUT> extends AutoProgressMapper<LongWritable, SqoopRecord, KEYOUT, VALOUT> {
    private Schema schema = null;
    private boolean bigDecimalFormatString = true;
    private LargeObjectLoader lobLoader = null;
    private boolean bigDecimalPadding;

    public ParquetImportMapperOverride() {
    }

    protected void setup(Mapper<LongWritable, SqoopRecord, KEYOUT, VALOUT>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.schema = this.getAvroSchema(conf);
        this.bigDecimalFormatString = conf.getBoolean("sqoop.bigdecimal.format.string", true);
        this.lobLoader = this.createLobLoader(context);
        GenericData.get().addLogicalTypeConversion(new DecimalConversion());
        this.bigDecimalPadding = conf.getBoolean("sqoop.avro.decimal_padding.enable", false);
    }

    protected void map(LongWritable key, SqoopRecord val, Mapper<LongWritable, SqoopRecord, KEYOUT, VALOUT>.Context context) throws IOException, InterruptedException {
        try {
            val.loadLargeObjects( this.lobLoader);
        } catch (SQLException var5) {
            throw new IOException(var5);
        }

        GenericRecord record = AvroUtilOverride.toGenericRecord(val.getFieldMap(), this.schema, this.bigDecimalFormatString, this.bigDecimalPadding);
        this.write(context, record);
    }

    protected void cleanup(Mapper<LongWritable, SqoopRecord, KEYOUT, VALOUT>.Context context) throws IOException {
        if (null != this.lobLoader) {
            this.lobLoader.close();
        }

    }

    protected abstract LargeObjectLoader createLobLoader(Mapper<LongWritable, SqoopRecord, KEYOUT, VALOUT>.Context var1) throws IOException, InterruptedException;

    protected abstract Schema getAvroSchema(Configuration var1);

    protected abstract void write(Mapper<LongWritable, SqoopRecord, KEYOUT, VALOUT>.Context var1, GenericRecord var2) throws IOException, InterruptedException;
}
