package plugin.decimal;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hbase.HBaseUtil;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.mapreduce.DataDrivenImportJob;
import com.cloudera.sqoop.mapreduce.HBaseImportJob;
import com.cloudera.sqoop.mapreduce.ImportJobBase;
import com.cloudera.sqoop.mapreduce.JdbcUpsertExportJob;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.accumulo.AccumuloUtil;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.ImportJobContext;
import org.apache.sqoop.manager.InformationSchemaManager;
import org.apache.sqoop.manager.MySQLManager;
import org.apache.sqoop.mapreduce.AccumuloImportJob;
import org.apache.sqoop.mapreduce.HBaseBulkImportJob;
import org.apache.sqoop.mapreduce.mysql.MySQLUpsertOutputFormat;
import org.apache.sqoop.util.LoggingUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月19日
 */
public class MySQLManagerOverride extends MySQLManager {

    public static final Log LOG = LogFactory.getLog(MySQLManager.class.getName());
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static boolean warningPrinted = false;
    private static final String EXPORT_OPERATION = "export";
    private Map<String, String> colTypeNames;
    private static final int YEAR_TYPE_OVERWRITE = 5;

    public MySQLManagerOverride(com.cloudera.sqoop.SqoopOptions opts) {
        super( opts);
    }

    protected void initOptionDefaults() {
        if (this.options.getFetchSize() == null) {
            LOG.info("Preparing to use a MySQL streaming resultset.");
            String operation = this.options.getToolName();
            if (StringUtils.isNotBlank(operation) && operation.equalsIgnoreCase("export")) {
                this.options.setFetchSize(0);
            } else {
                this.options.setFetchSize(-2147483648);
            }
        } else if (!this.options.getFetchSize().equals(-2147483648) && !this.options.getFetchSize().equals(0)) {
            LOG.info("Argument '--fetch-size " + this.options.getFetchSize() + "' will probably get ignored by MySQL JDBC driver.");
        }

    }

    protected String getPrimaryKeyQuery(String tableName) {
        return "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = (" + this.getSchemaQuery() + ") AND TABLE_NAME = '" + tableName + "' AND COLUMN_KEY = 'PRI'";
    }

    protected String getColNamesQuery(String tableName) {
        return "SELECT t.* FROM " + this.escapeTableName(tableName) + " AS t LIMIT 1";
    }


    @Override
    public void importTable(com.cloudera.sqoop.manager.ImportJobContext context) throws IOException, ImportException {
        String tableName = context.getTableName();
        String jarFile = context.getJarFile();
        SqoopOptions opts = context.getOptions();
        context.setConnManager(this);
        Object importer;
        if (opts.getHBaseTable() != null) {
            if (!HBaseUtil.isHBaseJarPresent()) {
                throw new ImportException("HBase jars are not present in classpath, cannot import to HBase!");
            }

            if (!opts.isBulkLoadEnabled()) {
                importer = new HBaseImportJob(opts, context);
            } else {
                importer = new HBaseBulkImportJob(opts, context);
            }
        } else if (opts.getAccumuloTable() != null) {
            if (!AccumuloUtil.isAccumuloJarPresent()) {
                throw new ImportException("Accumulo jars are not present in classpath, cannot import to Accumulo!");
            }

            importer = new AccumuloImportJob(opts, context);
        } else {
            importer = new DataDrivenImportJobOverride(opts, context.getInputFormat(), context);
        }

        this.checkTableImportOptions(context);
        String splitCol = this.getSplitColumn(opts, tableName);
        ((ImportJobBase)importer).runImport(tableName, jarFile, splitCol, opts.getConf());
    }

    public void upsertTable(ExportJobContext context) throws IOException, ExportException {
        context.setConnManager(this);
        LOG.warn("MySQL Connector upsert functionality is using INSERT ON");
        LOG.warn("DUPLICATE KEY UPDATE clause that relies on table's unique key.");
        LOG.warn("Insert/update distinction is therefore independent on column");
        LOG.warn("names specified in --update-key parameter. Please see MySQL");
        LOG.warn("documentation for additional limitations.");
        JdbcUpsertExportJob exportJob = new JdbcUpsertExportJob(context, MySQLUpsertOutputFormat.class);
        exportJob.runExport();
    }

    public void configureDbOutputColumns(SqoopOptions options) {
        if (options.getUpdateMode() != SqoopOptions.UpdateMode.AllowInsert) {
            super.configureDbOutputColumns(options);
        }
    }

    protected static void markWarningPrinted() {
        warningPrinted = true;
    }

    private void checkDateTimeBehavior(org.apache.sqoop.manager.ImportJobContext context) {
        String ZERO_BEHAVIOR_STR = "zeroDateTimeBehavior";
        String CONVERT_TO_NULL = "=convertToNull";
        String connectStr = context.getOptions().getConnectString();
        if (connectStr.indexOf("jdbc:") == 0) {
            String uriComponent = connectStr.substring(5);

            try {
                URI uri = new URI(uriComponent);
                String query = uri.getQuery();
                if (null == query) {
                    connectStr = connectStr + "?" + "zeroDateTimeBehavior" + "=convertToNull";
                    LOG.info("Setting zero DATETIME behavior to convertToNull (mysql)");
                } else if (query.length() == 0) {
                    connectStr = connectStr + "zeroDateTimeBehavior" + "=convertToNull";
                    LOG.info("Setting zero DATETIME behavior to convertToNull (mysql)");
                } else if (query.indexOf("zeroDateTimeBehavior") == -1) {
                    if (!connectStr.endsWith("&")) {
                        connectStr = connectStr + "&";
                    }

                    connectStr = connectStr + "zeroDateTimeBehavior" + "=convertToNull";
                    LOG.info("Setting zero DATETIME behavior to convertToNull (mysql)");
                }

                LOG.debug("Rewriting connect string to " + connectStr);
                context.getOptions().setConnectString(connectStr);
            } catch (URISyntaxException var8) {
                LOG.debug("mysql: Couldn't parse connect str in checkDateTimeBehavior: " + var8);
            }

        }
    }

    public void execAndPrint(String s) {
        ResultSet results = null;

        try {
            results = super.execute(s, 0, new Object[0]);
        } catch (SQLException var8) {
            LoggingUtils.logAll(LOG, "Error executing statement: ", var8);
            this.release();
            return;
        }

        PrintWriter pw = new PrintWriter(System.out, true);

        try {
            this.formatAndPrintResultSet(results, pw);
        } finally {
            pw.close();
        }

    }

    public String escapeColName(String colName) {
        return null == colName ? null : "`" + colName + "`";
    }

    public String escapeTableName(String tableName) {
        return null == tableName ? null : "`" + tableName + "`";
    }

    public boolean supportsStagingForExport() {
        return true;
    }

    public String[] getColumnNamesForProcedure(String procedureName) {
        ArrayList ret = new ArrayList();

        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            ResultSet results = metaData.getProcedureColumns((String)null, (String)null, procedureName, (String)null);
            if (null == results) {
                LOG.debug("Get Procedure Columns returns null");
                return null;
            } else {
                String[] var6;
                try {
                    while(results.next()) {
                        if (results.getInt("COLUMN_TYPE") != 5) {
                            String name = results.getString("COLUMN_NAME");
                            ret.add(name);
                        }
                    }

                    String[] result = (String[])ret.toArray(new String[ret.size()]);
                    LOG.debug("getColumnsNamesForProcedure returns " + StringUtils.join(ret, ","));
                    var6 = result;
                } finally {
                    results.close();
                    this.getConnection().commit();
                }

                return var6;
            }
        } catch (SQLException var11) {
            LoggingUtils.logAll(LOG, "Error reading procedure metadata: ", var11);
            throw new RuntimeException("Can't fetch column names for procedure.", var11);
        }
    }

    public Map<String, Integer> getColumnTypesForProcedure(String procedureName) {
        TreeMap ret = new TreeMap();

        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            ResultSet results = metaData.getProcedureColumns((String)null, (String)null, procedureName, (String)null);
            if (null == results) {
                LOG.debug("getColumnTypesForProcedure returns null");
                return null;
            } else {
                TreeMap var5;
                try {
                    while(results.next()) {
                        if (results.getInt("COLUMN_TYPE") != 5) {
                            ret.put(results.getString("COLUMN_NAME"), results.getInt("DATA_TYPE"));
                        }
                    }

                    LOG.debug("Columns returned = " + StringUtils.join(ret.keySet(), ","));
                    LOG.debug("Types returned = " + StringUtils.join(ret.values(), ","));
                    var5 = ret.isEmpty() ? null : ret;
                } finally {
                    if (results != null) {
                        results.close();
                    }

                    this.getConnection().commit();
                }

                return var5;
            }
        } catch (SQLException var10) {
            LoggingUtils.logAll(LOG, "Error reading primary key metadata: " + var10.toString(), var10);
            return null;
        }
    }

    public Map<String, String> getColumnTypeNamesForProcedure(String procedureName) {
        TreeMap ret = new TreeMap();

        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            ResultSet results = metaData.getProcedureColumns((String)null, (String)null, procedureName, (String)null);
            if (null == results) {
                LOG.debug("getColumnTypesForProcedure returns null");
                return null;
            } else {
                TreeMap var5;
                try {
                    while(results.next()) {
                        if (results.getInt("COLUMN_TYPE") != 5) {
                            ret.put(results.getString("COLUMN_NAME"), results.getString("TYPE_NAME"));
                        }
                    }

                    LOG.debug("Columns returned = " + StringUtils.join(ret.keySet(), ","));
                    LOG.debug("Type names returned = " + StringUtils.join(ret.values(), ","));
                    var5 = ret.isEmpty() ? null : ret;
                } finally {
                    if (results != null) {
                        results.close();
                    }

                    this.getConnection().commit();
                }

                return var5;
            }
        } catch (SQLException var10) {
            LoggingUtils.logAll(LOG, "Error reading primary key metadata: " + var10.toString(), var10);
            return null;
        }
    }

    protected String getListDatabasesQuery() {
        return "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA";
    }

    protected String getSchemaQuery() {
        return "SELECT SCHEMA()";
    }

    private int overrideSqlType(String tableName, String columnName, int sqlType) {
        if (this.colTypeNames == null) {
            this.colTypeNames = this.getColumnTypeNames(tableName, this.options.getCall(), this.options.getSqlQuery());
        }

        if ("YEAR".equalsIgnoreCase((String)this.colTypeNames.get(columnName))) {
            sqlType = 5;
        }

        return sqlType;
    }

    public String toJavaType(String tableName, String columnName, int sqlType) {
        sqlType = this.overrideSqlType(tableName, columnName, sqlType);
        return super.toJavaType(tableName, columnName, sqlType);
    }

    public String toHiveType(String tableName, String columnName, int sqlType) {
        sqlType = this.overrideSqlType(tableName, columnName, sqlType);
        return super.toHiveType(tableName, columnName, sqlType);
    }

    public Schema.Type toAvroType(String tableName, String columnName, int sqlType) {
        sqlType = this.overrideSqlType(tableName, columnName, sqlType);
        return super.toAvroType(tableName, columnName, sqlType);
    }
}