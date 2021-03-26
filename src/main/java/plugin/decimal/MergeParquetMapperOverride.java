package plugin.decimal;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.avro.AvroUtil;
import com.cloudera.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.MergeJob;
import org.apache.sqoop.mapreduce.MergeRecord;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class MergeParquetMapperOverride extends MergeGenericRecordExportMapper<GenericRecord, GenericRecord> {

    /**
     * 日志
     */
    public static final Log LOG = LogFactory.getLog(MergeParquetMapperOverride.class.getName());

    private Map<String, Pair<String, String>> sqoopRecordFields = new HashMap<String, Pair<String, String>>();
    private SqoopRecord sqoopRecordImpl;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        final String userClassName = conf.get(MergeJob.MERGE_SQOOP_RECORD_KEY);
        try {
            final Class<? extends Object> clazz = Class.forName(userClassName, true,
                    Thread.currentThread().getContextClassLoader());
            sqoopRecordImpl = (SqoopRecord) ReflectionUtils.newInstance(clazz, conf);
            for (final Field field : clazz.getDeclaredFields()) {
                final String fieldName = field.getName();
                final String fieldTypeName = field.getType().getName();
                sqoopRecordFields.put(fieldName.toLowerCase(), new Pair<String, String>(fieldName,
                        fieldTypeName));
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Cannot find the user record class with class name"
                    + userClassName, e);
        }
    }

    @Override
    protected void map(GenericRecord key, GenericRecord val, Mapper.Context context)
            throws IOException, InterruptedException {
        processRecord(toSqoopRecord(val), context);
    }


    private SqoopRecord toSqoopRecord(GenericRecord genericRecord) throws IOException {
        Schema avroSchema = genericRecord.getSchema();
        for (Schema.Field field : avroSchema.getFields()) {
            Pair<String, String> sqoopRecordField = sqoopRecordFields.get(field.name().toLowerCase());
            if (null == sqoopRecordField) {
                throw new IOException("Cannot find field '" + field.name() + "' in fields of user class"
                        + sqoopRecordImpl.getClass().getName() + ". Fields are: "
                        + Arrays.deepToString(sqoopRecordFields.values().toArray()));
            }
            Object avroObject = genericRecord.get(field.name());
            Object fieldVal = AvroUtil.fromAvro(avroObject, field.schema(), sqoopRecordField.value());
            Schema singleSchema = getSingleSchema(field.schema());
            if (singleSchema != null && singleSchema.getType() == Schema.Type.FIXED) {
                if (AvroUtilOverride.DECIMAL.equals(singleSchema.getProp(LogicalType.LOGICAL_TYPE_PROP))) {
                    LogicalTypes.Decimal logicalType = (LogicalTypes.Decimal) singleSchema.getLogicalType();
                    if (fieldVal != null) {
                        fieldVal = new BigDecimal(new BigInteger(((GenericFixed) fieldVal).bytes()), logicalType.getScale());
                    }
                }
            }
            sqoopRecordImpl.setField(sqoopRecordField.key(), fieldVal);
        }
        if (LOG.isDebugEnabled()) {
            LOG.info("toSqoopRecord:" + sqoopRecordImpl.toString());
        }
        return sqoopRecordImpl;
    }

    /**
     * @param schema
     * @return
     */
    private Schema getSingleSchema(Schema schema) {
        Schema result;
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> types = schema.getTypes();
            if (types.size() != 2) {
                throw new IllegalArgumentException("Only support union with null");
            } else {
                Schema s1 = (Schema) types.get(0);
                Schema s2 = (Schema) types.get(1);
                if (s1.getType() == Schema.Type.NULL) {
                    result = s2;
                } else {
                    if (s2.getType() == Schema.Type.NULL) {
                        result = s1;
                    }
                    throw new IllegalArgumentException("Only support union with null");
                }
            }
        } else {
            result = schema;
        }
        return result;
    }

    /**
     * fixed转decimal
     *
     * @param bytes
     * @return
     */
    private BigDecimal binaryToDecimal(byte[] bytes, int precision, int scale) {
        if (precision > 18) {
            throw new IllegalArgumentException("Original data type is fixed and now is binary,please check you table schema.");
        }
        BigDecimal result;
        int start = 0;//buffer.arrayOffset() + buffer.position();
        int end = bytes.length; //buffer.arrayOffset() + buffer.limit();
        long unscaled = 0L;
        int i = start;
        while (i < end) {
            unscaled = (unscaled << 8 | bytes[i] & 0xff);
            i++;
        }
        int bits = 8 * (end - start);
        long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
        if (unscaledNew <= -Math.pow(10, 18) || unscaledNew >= Math.pow(10, 18)) {
            result = new BigDecimal(unscaledNew);
        } else {
            result = BigDecimal.valueOf(unscaledNew / Math.pow(10, scale));
        }
        return result;
    }
}