package plugin.decimal;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.GenericDataSupplier;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.sqoop.mapreduce.*;
import com.cloudera.sqoop.orm.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;
import org.apache.sqoop.util.FileSystemUtil;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月21日
 */
public class DataDrivenImportJobOverride extends DataDrivenImportJob{

    public static final Log LOG = LogFactory.getLog(DataDrivenImportJobOverride.class.getName());

    public DataDrivenImportJobOverride(com.cloudera.sqoop.SqoopOptions opts) {
        super(opts);
    }

    public DataDrivenImportJobOverride(SqoopOptions opts, Class<? extends InputFormat> inputFormatClass, ImportJobContext context) {
        super(opts,inputFormatClass,context);
    }

    protected void configureMapper(Job job, String tableName, String tableClassName) throws IOException {
        if (options.getConf().getBoolean("sqoop.parquet.logical_types.decimal.enable", false)) {
            // Kite SDK requires an Avro schema to represent the data structure of
            // target dataset. If the schema name equals to generated java class name,
            // the import will fail. So we use table name as schema name and add a
            // prefix "codegen_" to generated java class to avoid the conflict.
            final String schemaNameOverride = tableName;
            Schema schema = generateAvroSchema(tableName, schemaNameOverride);
            Path destination = getContext().getDestination();
            // 将参数放入context中
            options.getConf().set("parquetjob.avro.schema", schema.toString());
            options.getConf().set("","");
            LOG.info("schema Info:" + schema);
            LOG.info("conf Info:" + options.getConf().get("parquetjob.avro.schema"));
            LOG.info("parse info" + new Schema.Parser().parse(options.getConf().get("parquetjob.avro.schema")));

            // 代替configMapper
            // 1 configureAvroSchema
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using Avro schema: " + schema);
            }

            AvroParquetOutputFormat.setSchema(job, schema);
            ContextUtil.getConfiguration(job).set("parquetjob.avro.schema",schema.toString());


            // 2 configureOutputCodec
            String outputCodec = job.getConfiguration().get("parquetjob.output.codec");
            if (outputCodec != null) {
                LOG.info("Using output codec: " + outputCodec);
                AvroParquetOutputFormat.setCompression(job, CompressionCodecName.fromConf(outputCodec));
            }

            // 3 configureLogicalTypeSupport
            if (options.getConf().getBoolean("sqoop.parquet.logical_types.decimal.enable", false)) {
                AvroParquetOutputFormat.setAvroDataSupplier(job, GenericDataSupplier.class);
            }

//            // 添加输出类
//            job.setOutputFormatClass(AvroParquetOutputFormat.class);

//            parquetImportJobConfiguratorOverride.configureMapper(job, schema, options, tableName, destination);

            // 配置 ParquetImportMapper 而不是 HadoopParquetImportMapper
            job.setMapperClass(getMapperClass());
        } else {
            super.configureMapper(job, tableName, tableClassName);
        }
    }



    private Schema generateAvroSchema(String tableName, String schemaNameOverride) throws IOException {
        com.cloudera.sqoop.manager.ConnManager connManager = this.getContext().getConnManager();
        AvroSchemaGeneratorOverride generator = new AvroSchemaGeneratorOverride(this.options, connManager, tableName);
        return generator.generate(schemaNameOverride);
    }



    protected Class<? extends Mapper> getMapperClass() {
        if (this.options.getHCatTableName() != null) {
            return SqoopHCatUtilities.getImportMapperClass();
        } else if (this.options.getFileLayout() == SqoopOptions.FileLayout.TextFile) {
            return TextImportMapper.class;
        } else if (this.options.getFileLayout() == SqoopOptions.FileLayout.SequenceFile) {
            return SequenceFileImportMapper.class;
        } else if (this.options.getFileLayout() == SqoopOptions.FileLayout.AvroDataFile) {
            return AvroImportMapper.class;
        } else {
            return this.options.getFileLayout() == SqoopOptions.FileLayout.ParquetFile ? HadoopParquetImportMapperOverride.class : null;
        }
    }

    protected Class<? extends OutputFormat> getOutputFormatClass() throws ClassNotFoundException {
        if (this.isHCatJob) {
            LOG.debug("Returning HCatOutputFormat for output format");
            return SqoopHCatUtilities.getOutputFormatClass();
        } else if (this.options.getFileLayout() == SqoopOptions.FileLayout.TextFile) {
            return RawKeyTextOutputFormat.class;
        } else if (this.options.getFileLayout() == SqoopOptions.FileLayout.SequenceFile) {
            return SequenceFileOutputFormat.class;
        } else if (this.options.getFileLayout() == SqoopOptions.FileLayout.AvroDataFile) {
            return AvroOutputFormat.class;
        } else {
            // DatasetKeyOutputFormat 被替换成 AvroParquetOutputFormat
            return this.options.getFileLayout() == SqoopOptions.FileLayout.ParquetFile ? AvroParquetOutputFormat.class : null;
        }
    }

    private String buildBoundaryQuery(String col, String query) {
        if (col != null && this.options.getNumMappers() != 1) {
            String alias = "t1";
            int dot = col.lastIndexOf(46);
            String qualifiedName = dot == -1 ? col : alias + col.substring(dot);
            com.cloudera.sqoop.manager.ConnManager mgr = this.getContext().getConnManager();
            String ret = mgr.getInputBoundsQuery(qualifiedName, query);
            return ret != null ? ret : "SELECT MIN(" + qualifiedName + "), MAX(" + qualifiedName + ") FROM (" + query + ") AS " + alias;
        } else {
            return "";
        }
    }

    protected void configureInputFormat(Job job, String tableName, String tableClassName, String splitByCol) throws IOException {
        ConnManager mgr = this.getContext().getConnManager();

        try {
            String username = this.options.getUsername();
            if (null != username && username.length() != 0) {
                DBConfiguration.configureDB(job.getConfiguration(), mgr.getDriverClass(), this.options.getConnectString(), username, this.options.getPassword(), this.options.getFetchSize(), this.options.getConnectionParams());
            } else {
                DBConfiguration.configureDB(job.getConfiguration(), mgr.getDriverClass(), this.options.getConnectString(), this.options.getFetchSize(), this.options.getConnectionParams());
            }

            String whereClause;
            if (null != tableName) {
                String[] colNames = this.options.getColumns();
                if (null == colNames) {
                    colNames = mgr.getColumnNames(tableName);
                }

                String[] sqlColNames = null;
                if (null != colNames) {
                    sqlColNames = new String[colNames.length];

                    for(int i = 0; i < colNames.length; ++i) {
                        sqlColNames[i] = mgr.escapeColName(colNames[i]);
                    }
                }

                whereClause = this.options.getWhereClause();
                DataDrivenDBInputFormat.setInput(job, DBWritable.class, mgr.escapeTableName(tableName), whereClause, mgr.escapeColName(splitByCol), sqlColNames);
                if (this.options.getBoundaryQuery() != null) {
                    DataDrivenDBInputFormat.setBoundingQuery(job.getConfiguration(), this.options.getBoundaryQuery());
                }
            } else {
                String inputQuery = this.options.getSqlQuery();
                String sanitizedQuery = inputQuery.replace("$CONDITIONS", " (1 = 1) ");
                whereClause = this.options.getBoundaryQuery();
                if (whereClause == null) {
                    whereClause = this.buildBoundaryQuery(splitByCol, sanitizedQuery);
                }

                DataDrivenDBInputFormat.setInput(job, DBWritable.class, inputQuery, whereClause);
                (new DBConfiguration(job.getConfiguration())).setInputOrderBy(splitByCol);
            }

            if (this.options.getRelaxedIsolation()) {
                LOG.info("Enabling relaxed (read uncommitted) transaction isolation for imports");
                job.getConfiguration().setBoolean("org.apache.sqoop.db.relaxedisolation", true);
            }

            LOG.debug("Using table class: " + tableClassName);
            job.getConfiguration().set(ConfigurationHelper.getDbInputClassProperty(), tableClassName);
            job.getConfiguration().setLong("sqoop.inline.lob.length.max", this.options.getInlineLobLimit());
            if (this.options.getSplitLimit() != null) {
                org.apache.sqoop.config.ConfigurationHelper.setSplitLimit(job.getConfiguration(), (long)this.options.getSplitLimit());
            }

            LOG.debug("Using InputFormat: " + this.inputFormatClass);
            job.setInputFormatClass(this.inputFormatClass);
        } finally {
            try {
                mgr.close();
            } catch (SQLException var15) {
                LOG.warn("Error closing connection: " + var15);
            }

        }

    }



}