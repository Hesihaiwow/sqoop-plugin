package plugin.decimal;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.orm.ClassWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class AvroUtilOverride {

    public static final String DECIMAL = "decimal";
    private static final String TIMESTAMP_TYPE = "java.sql.Timestamp";
    private static final String TIME_TYPE = "java.sql.Time";
    private static final String DATE_TYPE = "java.sql.Date";
    private static final String BIG_DECIMAL_TYPE = "java.math.BigDecimal";
    private static final String BLOB_REF_TYPE = "org.apache.sqoop.lib.BlobRef";

    public AvroUtilOverride() {
    }

    public static boolean isDecimal(Schema.Field field) {
        return isDecimal(field.schema());
    }

    public static boolean isDecimal(Schema schema) {
        if (schema.getType().equals(Schema.Type.UNION)) {
            Iterator var1 = schema.getTypes().iterator();

            Schema type;
            do {
                if (!var1.hasNext()) {
                    return false;
                }

                type = (Schema)var1.next();
            } while(!isDecimal(type));

            return true;
        } else {
            return "decimal".equals(schema.getProp("logicalType"));
        }
    }

    private static BigDecimal padBigDecimal(BigDecimal bd, Schema schema) {
        Schema schemaContainingScale = getDecimalSchema(schema);
        if (schemaContainingScale != null) {
            int scale = Integer.valueOf(schemaContainingScale.getObjectProp("scale").toString());
            if (bd.scale() != scale) {
                return bd.setScale(scale);
            }
        }

        return bd;
    }

    private static Schema getDecimalSchema(Schema schema) {
        if (schema.getType().equals(Schema.Type.UNION)) {
            Iterator var1 = schema.getTypes().iterator();

            while(var1.hasNext()) {
                Schema type = (Schema)var1.next();
                Schema schemaContainingScale = getDecimalSchema(type);
                if (schemaContainingScale != null) {
                    return schemaContainingScale;
                }
            }
        } else if ("decimal".equals(schema.getProp("logicalType"))) {
            return schema;
        }

        return null;
    }

    public static Object toAvro(Object o, Schema.Field field, boolean bigDecimalFormatString, boolean bigDecimalPaddingEnabled) {
        if (o instanceof BigDecimal) {
            if (bigDecimalPaddingEnabled) {
                o = padBigDecimal((BigDecimal)o, field.schema());
            }

            if (!isDecimal(field)) {
                if (bigDecimalFormatString) {
                    return ((BigDecimal)o).toPlainString();
                }

                return o.toString();
            }
        } else {
            if (o instanceof Date) {
                return ((Date)o).getTime();
            }

            if (o instanceof Time) {
                return ((Time)o).getTime();
            }

            if (o instanceof Timestamp) {
                return ((Timestamp)o).getTime();
            }

            if (o instanceof BytesWritable) {
                BytesWritable bw = (BytesWritable)o;
                return ByteBuffer.wrap(bw.getBytes(), 0, bw.getLength());
            }

            if (o instanceof BlobRef) {
                BlobRef br = (BlobRef)o;
                byte[] bytes = br.isExternal() ? br.toString().getBytes() : (byte[])br.getData();
                return ByteBuffer.wrap(bytes);
            }

            if (o instanceof ClobRef) {
                throw new UnsupportedOperationException("ClobRef not supported");
            }
        }

        return o;
    }

    public static String toAvroColumn(String column) {
        String candidate = ClassWriter.toJavaIdentifier(column);
        return toAvroIdentifier(candidate);
    }

    public static String toAvroIdentifier(String candidate) {
        char[] data = candidate.toCharArray();
        boolean skip = false;
        int stringIndex = 0;
        char[] var4 = data;
        int var5 = data.length;

        for(int var6 = 0; var6 < var5; ++var6) {
            char c = var4[var6];
            if (!Character.isLetterOrDigit(c) && c != '_') {
                if (!skip) {
                    data[stringIndex++] = '_';
                    skip = true;
                }
            } else {
                data[stringIndex++] = c;
                skip = false;
            }
        }

        char initial = data[0];
        if (!Character.isLetter(initial) && initial != '_') {
            return "AVRO_".concat(new String(data, 0, stringIndex));
        } else {
            return new String(data, 0, stringIndex);
        }
    }

    public static GenericRecord toGenericRecord(Map<String, Object> fieldMap, Schema schema, boolean bigDecimalFormatString) {
        return toGenericRecord(fieldMap, schema, bigDecimalFormatString, false);
    }

    public static GenericRecord toGenericRecord(Map<String, Object> fieldMap, Schema schema, boolean bigDecimalFormatString, boolean bigDecimalPaddingEnabled) {
        GenericRecord record = new GenericData.Record(schema);
        Iterator var5 = fieldMap.entrySet().iterator();

        while(var5.hasNext()) {
            Map.Entry<String, Object> entry = (Map.Entry)var5.next();
            String avroColumn = toAvroColumn((String)entry.getKey());
            Schema.Field field = schema.getField(avroColumn);
            Object avroObject = toAvro(entry.getValue(), field, bigDecimalFormatString, bigDecimalPaddingEnabled);
            record.put(avroColumn, avroObject);
        }

        return record;
    }

    public static Object fromAvro(Object avroObject, Schema schema, String type) {
        if (avroObject == null) {
            return null;
        } else {
            switch(schema.getType()) {
                case NULL:
                    return null;
                case BOOLEAN:
                case INT:
                case FLOAT:
                case DOUBLE:
                    return avroObject;
                case LONG:
                    if (type.equals("java.sql.Date")) {
                        return new Date((Long)avroObject);
                    } else if (type.equals("java.sql.Time")) {
                        return new Time((Long)avroObject);
                    } else {
                        if (type.equals("java.sql.Timestamp")) {
                            return new Timestamp((Long)avroObject);
                        }

                        return avroObject;
                    }
                case BYTES:
                    if (isDecimal(schema)) {
                        return avroObject;
                    } else {
                        ByteBuffer bb = (ByteBuffer)avroObject;
                        BytesWritable bw = new BytesWritable();
                        bw.set(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
                        if (type.equals("org.apache.sqoop.lib.BlobRef")) {
                            throw new UnsupportedOperationException("BlobRef not supported");
                        }

                        return bw;
                    }
                case STRING:
                    if (type.equals("java.math.BigDecimal")) {
                        return new BigDecimal(avroObject.toString());
                    } else if (type.equals("java.sql.Date")) {
                        return Date.valueOf(avroObject.toString());
                    } else if (type.equals("java.sql.Time")) {
                        return Time.valueOf(avroObject.toString());
                    } else {
                        if (type.equals("java.sql.Timestamp")) {
                            return Timestamp.valueOf(avroObject.toString());
                        }

                        return avroObject.toString();
                    }
                case ENUM:
                    return avroObject.toString();
                case UNION:
                    List<Schema> types = schema.getTypes();
                    if (types.size() != 2) {
                        throw new IllegalArgumentException("Only support union with null");
                    } else {
                        Schema s1 = (Schema)types.get(0);
                        Schema s2 = (Schema)types.get(1);
                        if (s1.getType() == Schema.Type.NULL) {
                            return fromAvro(avroObject, s2, type);
                        } else {
                            if (s2.getType() == Schema.Type.NULL) {
                                return fromAvro(avroObject, s1, type);
                            }

                            throw new IllegalArgumentException("Only support union with null");
                        }
                    }
                case FIXED:
                    if (isDecimal(schema)) {
                        return avroObject;
                    }

                    return new BytesWritable(((GenericFixed)avroObject).bytes());
                case RECORD:
                case ARRAY:
                case MAP:
                default:
                    throw new IllegalArgumentException("Cannot convert Avro type " + schema.getType());
            }
        }
    }

    public static Schema getAvroSchema(Path path, Configuration conf) throws IOException {
        Path fileToTest = getFileToTest(path, conf);
        SeekableInput input = new FsInput(fileToTest, conf);
        DatumReader<GenericRecord> reader = new GenericDatumReader();
        FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);
        Schema result = fileReader.getSchema();
        fileReader.close();
        return result;
    }

    public static LogicalType createDecimalType(Integer precision, Integer scale, Configuration conf) {


        return LogicalTypes.decimal(precision, scale);
    }

    private static Path getFileToTest(Path path, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.isDirectory(path)) {
            return path;
        } else {
            FileStatus[] fileStatuses = fs.listStatus(path, new PathFilter() {
                public boolean accept(Path p) {
                    String name = p.getName();
                    return !name.startsWith("_") && !name.startsWith(".");
                }
            });
            return fileStatuses.length == 0 ? null : fileStatuses[0].getPath();
        }
    }

    public static Schema parseAvroSchema(String schemaString) {
        return (new Schema.Parser()).parse(schemaString);
    }

    public static Schema getAvroSchemaFromParquetFile(Path path, Configuration conf) throws IOException {
        Path fileToTest = getFileToTest(path, conf);
        if (fileToTest == null) {
            return null;
        } else {
            ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(conf, fileToTest, ParquetMetadataConverter.NO_FILTER);
            MessageType parquetSchema = parquetMetadata.getFileMetaData().getSchema();
            AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
            return avroSchemaConverter.convert(parquetSchema);
        }
    }
}