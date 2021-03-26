package plugin.decimal;

import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月22日
 */
public class MergeParquetReducerOverride extends MergeParquetReducer {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //配置BigDecimal解析器
        SpecificData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());
    }

    @Override
    protected void write(Context context, GenericRecord record) throws IOException, InterruptedException {
        context.write(null, record);
    }
}