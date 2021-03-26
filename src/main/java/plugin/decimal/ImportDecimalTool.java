package plugin.decimal;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.cli.RelatedOptions;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.hive.HiveImport;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.MergeJob;
import com.cloudera.sqoop.metastore.JobData;
import com.cloudera.sqoop.metastore.JobStorage;
import com.cloudera.sqoop.metastore.JobStorageFactory;
import com.cloudera.sqoop.orm.TableClassName;
import org.apache.sqoop.manager.MySQLManager;
import org.apache.sqoop.orm.ClassWriter;
import org.apache.sqoop.tool.BaseSqoopTool;
import com.cloudera.sqoop.util.AppendUtils;
import com.cloudera.sqoop.util.ClassLoaderStack;
import com.cloudera.sqoop.util.ImportException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.avro.AvroSchemaMismatchException;
import org.apache.sqoop.manager.SupportedManagers;
import org.apache.sqoop.tool.CodeGenTool;
import org.apache.sqoop.tool.ImportTool;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月19日
 */
public class ImportDecimalTool extends BaseSqoopTool {

    public static final Log LOG = LogFactory.getLog(ImportTool.class.getName());
    private static final String IMPORT_FAILED_ERROR_MSG = "Import failed: ";
    private CodeGenTool codeGenerator;
    private boolean allTables;
    private int checkColumnType;
    private ClassLoader prevClassLoader;


    public ImportDecimalTool() {
        this("import", false);
    }

    public ImportDecimalTool(String toolName, boolean allTables) {
        this(toolName, new CodeGenTool(), allTables);
    }

    public ImportDecimalTool(String toolName, CodeGenTool codeGenerator, boolean allTables) {
        super(toolName);
        this.prevClassLoader = null;
        this.codeGenerator = codeGenerator;
        this.allTables = allTables;
    }

    protected boolean init(SqoopOptions sqoopOpts) {
        boolean ret = super.init(sqoopOpts);
        this.codeGenerator.setManager(this.manager);
        return ret;
    }

    public List<String> getGeneratedJarFiles() {
        return this.codeGenerator.getGeneratedJarFiles();
    }

    private void loadJars(Configuration conf, String ormJarFile, String tableClassName) throws IOException {
        boolean isLocal = "local".equals(conf.get("mapreduce.jobtracker.address")) || "local".equals(conf.get("mapred.job.tracker"));
        if (isLocal) {
            this.prevClassLoader = ClassLoaderStack.addJarFile(ormJarFile, tableClassName);
        }

    }

    private void unloadJars() {
        if (null != this.prevClassLoader) {
            ClassLoaderStack.setCurrentClassLoader(this.prevClassLoader);
        }

    }

    private boolean isIncremental(SqoopOptions options) {
        return !options.getIncrementalMode().equals(SqoopOptions.IncrementalMode.None);
    }

    private void saveIncrementalState(SqoopOptions options) throws IOException {
        if (this.isIncremental(options)) {
            Map<String, String> descriptor = options.getStorageDescriptor();
            String jobName = options.getJobName();
            if (null != jobName && null != descriptor) {
                LOG.info("Saving incremental import state to the metastore");
                JobStorageFactory ssf = new JobStorageFactory(options.getConf());
                JobStorage storage = ssf.getJobStorage(descriptor);
                storage.open(descriptor);

                try {
                    JobData data = new JobData(options.getParent(), this);
                    storage.update(jobName, data);
                    LOG.info("Updated data for job: " + jobName);
                } finally {
                    storage.close();
                }
            } else {
                LOG.info("Incremental import complete! To run another incremental import of all data following this import, supply the following arguments:");
                SqoopOptions.IncrementalMode incrementalMode = options.getIncrementalMode();
                switch(incrementalMode) {
                    case AppendRows:
                        LOG.info(" --incremental append");
                        break;
                    case DateLastModified:
                        LOG.info(" --incremental lastmodified");
                        break;
                    default:
                        LOG.warn("Undefined incremental mode: " + incrementalMode);
                }

                LOG.info("  --check-column " + options.getIncrementalTestColumn());
                LOG.info("  --last-value " + options.getIncrementalLastValue());
                LOG.info("(Consider saving this with 'sqoop job --create')");
            }

        }
    }

    private Object getMaxColumnId(SqoopOptions options) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT MAX(");
        sb.append(this.manager.escapeColName(options.getIncrementalTestColumn()));
        sb.append(") FROM ");
        String query;
        if (options.getTableName() != null) {
            sb.append(this.manager.escapeTableName(options.getTableName()));
            String where = options.getWhereClause();
            if (null != where) {
                sb.append(" WHERE ");
                sb.append(where);
            }

            query = sb.toString();
        } else {
            sb.append("(");
            sb.append(options.getSqlQuery());
            sb.append(") sqoop_import_query_alias");
            query = sb.toString().replaceAll("\\$CONDITIONS", "(1 = 1)");
        }

        Connection conn = this.manager.getConnection();
        Statement s = null;
        ResultSet rs = null;

        Time var8;
        try {
            LOG.info("Maximal id query for free form incremental import: " + query);
            s = conn.createStatement();
            rs = s.executeQuery(query);
            ResultSetMetaData rsmd;
            if (!rs.next()) {
                LOG.warn("Unexpected: empty results for max value query?");
                rsmd = null;
                return rsmd;
            }

            rsmd = rs.getMetaData();
            this.checkColumnType = rsmd.getColumnType(1);
            if (this.checkColumnType == 93) {
                Timestamp var31 = rs.getTimestamp(1);
                return var31;
            }

            if (this.checkColumnType == 91) {
                Date var30 = rs.getDate(1);
                return var30;
            }

            if (this.checkColumnType != 92) {
                Object var29 = rs.getObject(1);
                return var29;
            }

            var8 = rs.getTime(1);
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
            } catch (SQLException var26) {
                LOG.warn("SQL Exception closing resultset: " + var26);
            }

            try {
                if (null != s) {
                    s.close();
                }
            } catch (SQLException var25) {
                LOG.warn("SQL Exception closing statement: " + var25);
            }

        }

        return var8;
    }

    private boolean isDateTimeColumn(int columnType) {
        return columnType == 93 || columnType == 91 || columnType == 92;
    }

    private boolean initIncrementalConstraints(SqoopOptions options, ImportJobContext context) throws ImportException, IOException {
        if (!this.isIncremental(options)) {
            return true;
        } else {
            SqoopOptions.IncrementalMode incrementalMode = options.getIncrementalMode();
            String nextIncrementalValue = null;
            Object nextVal;
            switch(incrementalMode) {
                case AppendRows:
                    try {
                        nextVal = this.getMaxColumnId(options);
                        if (this.isDateTimeColumn(this.checkColumnType)) {
                            nextIncrementalValue = nextVal == null ? null : this.manager.datetimeToQueryString(nextVal.toString(), this.checkColumnType);
                        } else {
                            if (this.manager.isCharColumn(this.checkColumnType)) {
                                throw new ImportException("Character column (" + options.getIncrementalTestColumn() + ") can not be used to determine which rows to incrementally import.");
                            }

                            nextIncrementalValue = nextVal == null ? null : nextVal.toString();
                        }
                        break;
                    } catch (SQLException var11) {
                        throw new IOException(var11);
                    }
                case DateLastModified:
                    if (options.getMergeKeyCol() == null && !options.isAppendMode()) {
                        Path outputPath = this.getOutputPath(options, context.getTableName(), false);
                        FileSystem fs = outputPath.getFileSystem(options.getConf());
                        if (fs.exists(outputPath)) {
                            throw new ImportException("--merge-key or --append is required when using --" + "incremental" + " lastmodified and the output directory exists.");
                        }
                    }

                    this.checkColumnType = (Integer)this.manager.getColumnTypes(options.getTableName(), options.getSqlQuery()).get(options.getIncrementalTestColumn());
                    nextVal = this.manager.getCurrentDbTimestamp();
                    if (null == nextVal) {
                        throw new IOException("Could not get current time from database");
                    }

                    nextIncrementalValue = this.manager.datetimeToQueryString(nextVal.toString(), this.checkColumnType);
                    break;
                default:
                    throw new ImportException("Undefined incremental import type: " + incrementalMode);
            }

            StringBuilder sb = new StringBuilder();
            String prevEndpoint = options.getIncrementalLastValue();
            if (this.isDateTimeColumn(this.checkColumnType) && null != prevEndpoint && !prevEndpoint.startsWith("'") && !prevEndpoint.endsWith("'")) {
                prevEndpoint = this.manager.datetimeToQueryString(prevEndpoint, this.checkColumnType);
            }

            String checkColName = this.manager.escapeColName(options.getIncrementalTestColumn());
            LOG.info("Incremental import based on column " + checkColName);
            if (null != prevEndpoint) {
                if (prevEndpoint.equals(nextIncrementalValue)) {
                    LOG.info("No new rows detected since last import.");
                    return false;
                }

                LOG.info("Lower bound value: " + prevEndpoint);
                sb.append(checkColName);
                switch(incrementalMode) {
                    case AppendRows:
                        sb.append(" > ");
                        break;
                    case DateLastModified:
                        sb.append(" >= ");
                        break;
                    default:
                        throw new ImportException("Undefined comparison");
                }

                sb.append(prevEndpoint);
                sb.append(" AND ");
            }

            if (null != nextIncrementalValue) {
                sb.append(checkColName);
                switch(incrementalMode) {
                    case AppendRows:
                        sb.append(" <= ");
                        break;
                    case DateLastModified:
                        sb.append(" < ");
                        break;
                    default:
                        throw new ImportException("Undefined comparison");
                }

                sb.append(nextIncrementalValue);
            } else {
                sb.append(checkColName);
                sb.append(" IS NULL ");
            }

            LOG.info("Upper bound value: " + nextIncrementalValue);
            String prevWhereClause;
            if (options.getTableName() != null) {
                prevWhereClause = options.getWhereClause();
                if (null != prevWhereClause) {
                    sb.append(" AND (");
                    sb.append(prevWhereClause);
                    sb.append(")");
                }

                String newConstraints = sb.toString();
                options.setWhereClause(newConstraints);
            } else {
                sb.append(" AND $CONDITIONS");
                prevWhereClause = options.getSqlQuery().replace("$CONDITIONS", sb.toString());
                options.setSqlQuery(prevWhereClause);
            }

            SqoopOptions recordOptions = options.getParent();
            if (null == recordOptions) {
                recordOptions = options;
            }

            recordOptions.setIncrementalLastValue(nextVal == null ? null : nextVal.toString());
            return true;
        }
    }

    protected void lastModifiedMerge(SqoopOptions options, ImportJobContext context) throws IOException {
        if (context.getDestination() != null) {
            Path userDestDir = this.getOutputPath(options, context.getTableName(), false);
            FileSystem fs = userDestDir.getFileSystem(options.getConf());
            if (fs.exists(context.getDestination())) {
                LOG.info("Final destination exists, will run merge job.");
                if (fs.exists(userDestDir)) {
                    String tableClassName = null;
                    if (!context.getConnManager().isORMFacilitySelfManaged()) {
                        tableClassName = (new TableClassName(options)).getClassForTable(context.getTableName());
                    }

                    Path destDir = this.getOutputPath(options, context.getTableName());
                    options.setExistingJarName(context.getJarFile());
                    options.setClassName(tableClassName);
                    options.setMergeOldPath(userDestDir.toString());
                    options.setMergeNewPath(context.getDestination().toString());
                    options.setTargetDir(destDir.toString());
                    this.loadJars(options.getConf(), context.getJarFile(), ClassWriter.toJavaIdentifier("codegen_" + context.getTableName()));
                    HadoopParquetMergeJobConfiguratorOverride parquetMergeJobConfigurator = new HadoopParquetMergeJobConfiguratorOverride();
                    MergeJobOverride mergeJob = new MergeJobOverride(options,parquetMergeJobConfigurator);
                    if (mergeJob.runMergeJob()) {
                        Path tmpDir = this.getOutputPath(options, context.getTableName());
                        fs.rename(userDestDir, tmpDir);
                        fs.rename(destDir, userDestDir);
                        fs.delete(tmpDir, true);
                    } else {
                        LOG.error("Merge MapReduce job failed!");
                    }

                    this.unloadJars();
                } else {
                    if (!fs.exists(userDestDir.getParent())) {
                        fs.mkdirs(userDestDir.getParent());
                    }

                    LOG.info("Moving data from temporary directory " + context.getDestination() + " to final destination " + userDestDir);
                    if (!fs.rename(context.getDestination(), userDestDir)) {
                        throw new RuntimeException("Couldn't move data from temporary directory " + context.getDestination() + " to final destination " + userDestDir);
                    }
                }
            }

        }
    }

    protected boolean importTable(SqoopOptions options, String tableName, HiveImport hiveImport) throws IOException, ImportException {
        String jarFile = null;
        jarFile = this.codeGenerator.generateORM(options, tableName);
        Path outputPath = this.getOutputPath(options, tableName);
        ImportJobContext context = new ImportJobContext(tableName, jarFile, options, outputPath);
        if (!this.initIncrementalConstraints(options, context)) {
            return false;
        } else {
            if (options.isDeleteMode()) {
                this.deleteTargetDir(context);
            }

            if (null != tableName) {
                this.manager.importTable(context);
            } else {
                this.manager.importQuery(context);
            }

            if (options.isAppendMode()) {
                AppendUtils app = new AppendUtils(context);
                app.append();
            } else if (options.getIncrementalMode() == SqoopOptions.IncrementalMode.DateLastModified) {
                this.lastModifiedMerge(options, context);
            }

            if (options.doHiveImport() && options.getFileLayout() != SqoopOptions.FileLayout.ParquetFile) {
                hiveImport.importTable(tableName, options.getHiveTableName(), false);
            }

            this.saveIncrementalState(options);
            return true;
        }
    }

    private void deleteTargetDir(ImportJobContext context) throws IOException {
        SqoopOptions options = context.getOptions();
        Path destDir = context.getDestination();
        FileSystem fs = destDir.getFileSystem(options.getConf());
        if (fs.exists(destDir)) {
            fs.delete(destDir, true);
            LOG.info("Destination directory " + destDir + " deleted.");
        } else {
            LOG.info("Destination directory " + destDir + " is not present, hence not deleting.");
        }
    }

    private Path getOutputPath(SqoopOptions options, String tableName) {
        return this.getOutputPath(options, tableName, options.isAppendMode() || options.getIncrementalMode().equals(SqoopOptions.IncrementalMode.DateLastModified));
    }

    private Path getOutputPath(SqoopOptions options, String tableName, boolean temp) {
        String hdfsWarehouseDir = options.getWarehouseDir();
        String hdfsTargetDir = options.getTargetDir();
        Path outputPath = null;
        if (temp) {
            String salt = tableName;
            if (tableName == null && options.getSqlQuery() != null) {
                salt = Integer.toHexString(options.getSqlQuery().hashCode());
            }

            outputPath = AppendUtils.getTempAppendDir(salt, options);
            LOG.debug("Using temporary folder: " + outputPath.getName());
        } else if (hdfsTargetDir != null) {
            outputPath = new Path(hdfsTargetDir);
        } else if (hdfsWarehouseDir != null) {
            outputPath = new Path(hdfsWarehouseDir, tableName);
        } else if (null != tableName) {
            outputPath = new Path(tableName);
        }

        return outputPath;
    }

    public int run(SqoopOptions options) {
        HiveImport hiveImport = null;
        if (this.allTables) {
            LOG.error("ImportTool.run() can only handle a single table.");
            return 1;
        } else if (!this.init(options)) {
            return 1;
        } else {
            this.manager = new MySQLManagerOverride(options);
            this.codeGenerator.setManager(this.manager);

            byte var4;
            try {
                if (options.doHiveImport()) {
                    hiveImport = new HiveImport(options, this.manager, options.getConf(), false);
                }

                this.importTable(options, options.getTableName(), hiveImport);
                return 0;
            } catch (IllegalArgumentException var11) {
                LOG.error("Import failed: " + var11.getMessage());
                this.rethrowIfRequired(options, var11);
                var4 = 1;
                return var4;
            } catch (IOException var12) {
                LOG.error("Import failed: " + StringUtils.stringifyException(var12));
                this.rethrowIfRequired(options, var12);
                var4 = 1;
                return var4;
            } catch (ImportException var13) {
                LOG.error("Import failed: " + var13.toString());
                this.rethrowIfRequired(options, var13);
                var4 = 1;
                return var4;
            } catch (AvroSchemaMismatchException var14) {
                LOG.error("Import failed: ", var14);
                this.rethrowIfRequired(options, var14);
                var4 = 1;
            } finally {
                this.destroy(options);
            }

            return var4;
        }
    }

    protected RelatedOptions getImportOptions() {
        RelatedOptions importOpts = new RelatedOptions("Import control arguments");
        OptionBuilder.withDescription("Use direct import fast path");
        OptionBuilder.withLongOpt("direct");
        importOpts.addOption(OptionBuilder.create());
        if (!this.allTables) {
            OptionBuilder.withArgName("table-name");
            OptionBuilder.hasArg();
            OptionBuilder.withDescription("Table to read");
            OptionBuilder.withLongOpt("table");
            importOpts.addOption(OptionBuilder.create());
            OptionBuilder.withArgName("col,col,col...");
            OptionBuilder.hasArg();
            OptionBuilder.withDescription("Columns to import from table");
            OptionBuilder.withLongOpt("columns");
            importOpts.addOption(OptionBuilder.create());
            OptionBuilder.withArgName("column-name");
            OptionBuilder.hasArg();
            OptionBuilder.withDescription("Column of the table used to split work units");
            OptionBuilder.withLongOpt("split-by");
            importOpts.addOption(OptionBuilder.create());
            OptionBuilder.withArgName("size");
            OptionBuilder.hasArg();
            OptionBuilder.withDescription("Upper Limit of rows per split for split columns of Date/Time/Timestamp and integer types. For date or timestamp fields it is calculated in seconds. split-limit should be greater than 0");
            OptionBuilder.withLongOpt("split-limit");
            importOpts.addOption(OptionBuilder.create());
            OptionBuilder.withArgName("where clause");
            OptionBuilder.hasArg();
            OptionBuilder.withDescription("WHERE clause to use during import");
            OptionBuilder.withLongOpt("where");
            importOpts.addOption(OptionBuilder.create());
            OptionBuilder.withDescription("Imports data in append mode");
            OptionBuilder.withLongOpt("append");
            importOpts.addOption(OptionBuilder.create());
            OptionBuilder.withDescription("Imports data in delete mode");
            OptionBuilder.withLongOpt("delete-target-dir");
            importOpts.addOption(OptionBuilder.create());
            OptionBuilder.withArgName("dir");
            OptionBuilder.hasArg();
            OptionBuilder.withDescription("HDFS plain table destination");
            OptionBuilder.withLongOpt("target-dir");
            importOpts.addOption(OptionBuilder.create());
            OptionBuilder.withArgName("statement");
            OptionBuilder.hasArg();
            OptionBuilder.withDescription("Import results of SQL 'statement'");
            OptionBuilder.withLongOpt("query");
            importOpts.addOption(OptionBuilder.create("e"));
            OptionBuilder.withArgName("statement");
            OptionBuilder.hasArg();
            OptionBuilder.withDescription("Set boundary query for retrieving max and min value of the primary key");
            OptionBuilder.withLongOpt("boundary-query");
            importOpts.addOption(OptionBuilder.create());
            OptionBuilder.withArgName("column");
            OptionBuilder.hasArg();
            OptionBuilder.withDescription("Key column to use to join results");
            OptionBuilder.withLongOpt("merge-key");
            importOpts.addOption(OptionBuilder.create());
            this.addValidationOpts(importOpts);
        }

        OptionBuilder.withArgName("dir");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("HDFS parent for table destination");
        OptionBuilder.withLongOpt("warehouse-dir");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withDescription("Imports data to SequenceFiles");
        OptionBuilder.withLongOpt("as-sequencefile");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withDescription("Imports data as plain text (default)");
        OptionBuilder.withLongOpt("as-textfile");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withDescription("Imports data to Avro data files");
        OptionBuilder.withLongOpt("as-avrodatafile");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withDescription("Imports data to Parquet files");
        OptionBuilder.withLongOpt("as-parquetfile");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withArgName("n");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Use 'n' map tasks to import in parallel");
        OptionBuilder.withLongOpt("num-mappers");
        importOpts.addOption(OptionBuilder.create("m"));
        OptionBuilder.withArgName("name");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Set name for generated mapreduce job");
        OptionBuilder.withLongOpt("mapreduce-job-name");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withDescription("Enable compression");
        OptionBuilder.withLongOpt("compress");
        importOpts.addOption(OptionBuilder.create("z"));
        OptionBuilder.withArgName("codec");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Compression codec to use for import");
        OptionBuilder.withLongOpt("compression-codec");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withArgName("n");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Split the input stream every 'n' bytes when importing in direct mode");
        OptionBuilder.withLongOpt("direct-split-size");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withArgName("n");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Set the maximum size for an inline LOB");
        OptionBuilder.withLongOpt("inline-lob-limit");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withArgName("n");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Set number 'n' of rows to fetch from the database when more rows are needed");
        OptionBuilder.withLongOpt("fetch-size");
        importOpts.addOption(OptionBuilder.create());
        OptionBuilder.withArgName("reset-mappers");
        OptionBuilder.withDescription("Reset the number of mappers to one mapper if no split key available");
        OptionBuilder.withLongOpt("autoreset-to-one-mapper");
        importOpts.addOption(OptionBuilder.create());
        return importOpts;
    }

    protected RelatedOptions getIncrementalOptions() {
        RelatedOptions incrementalOpts = new RelatedOptions("Incremental import arguments");
        OptionBuilder.withArgName("import-type");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Define an incremental import of type 'append' or 'lastmodified'");
        OptionBuilder.withLongOpt("incremental");
        incrementalOpts.addOption(OptionBuilder.create());
        OptionBuilder.withArgName("column");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Source column to check for incremental change");
        OptionBuilder.withLongOpt("check-column");
        incrementalOpts.addOption(OptionBuilder.create());
        OptionBuilder.withArgName("value");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Last imported value in the incremental check column");
        OptionBuilder.withLongOpt("last-value");
        incrementalOpts.addOption(OptionBuilder.create());
        return incrementalOpts;
    }

    public void configureOptions(ToolOptions toolOptions) {
        toolOptions.addUniqueOptions(this.getCommonOptions());
        toolOptions.addUniqueOptions(this.getImportOptions());
        if (!this.allTables) {
            toolOptions.addUniqueOptions(this.getIncrementalOptions());
        }

        toolOptions.addUniqueOptions(this.getOutputFormatOptions());
        toolOptions.addUniqueOptions(this.getInputFormatOptions());
        toolOptions.addUniqueOptions(this.getHiveOptions(true));
        toolOptions.addUniqueOptions(this.getHBaseOptions());
        toolOptions.addUniqueOptions(this.getHCatalogOptions());
        toolOptions.addUniqueOptions(this.getHCatImportOnlyOptions());
        toolOptions.addUniqueOptions(this.getAccumuloOptions());
        RelatedOptions codeGenOpts = this.getCodeGenOpts(this.allTables);
        OptionBuilder.withArgName("file");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Disable code generation; use specified jar");
        OptionBuilder.withLongOpt("jar-file");
        codeGenOpts.addOption(OptionBuilder.create());
        toolOptions.addUniqueOptions(codeGenOpts);
    }

    public void printHelp(ToolOptions toolOptions) {
        super.printHelp(toolOptions);
        System.out.println("");
        if (this.allTables) {
            System.out.println("At minimum, you must specify --connect");
        } else {
            System.out.println("At minimum, you must specify --connect and --table");
        }

        System.out.println("Arguments to mysqldump and other subprograms may be supplied");
        System.out.println("after a '--' on the command line.");
    }

    private void applyIncrementalOptions(CommandLine in, SqoopOptions out) throws SqoopOptions.InvalidOptionsException {
        if (in.hasOption("incremental")) {
            String incrementalTypeStr = in.getOptionValue("incremental");
            if ("append".equals(incrementalTypeStr)) {
                out.setIncrementalMode(SqoopOptions.IncrementalMode.AppendRows);
                out.setAppendMode(true);
            } else {
                if (!"lastmodified".equals(incrementalTypeStr)) {
                    throw new SqoopOptions.InvalidOptionsException("Unknown incremental import mode: " + incrementalTypeStr + ". Use 'append' or 'lastmodified'." + "\nTry --help for usage instructions.");
                }

                out.setIncrementalMode(SqoopOptions.IncrementalMode.DateLastModified);
            }
        }

        if (in.hasOption("check-column")) {
            out.setIncrementalTestColumn(in.getOptionValue("check-column"));
        }

        if (in.hasOption("last-value")) {
            out.setIncrementalLastValue(in.getOptionValue("last-value"));
        }

    }

    public void applyOptions(CommandLine in, SqoopOptions out) throws SqoopOptions.InvalidOptionsException {
        try {
            this.applyCommonOptions(in, out);
            if (in.hasOption("direct")) {
                out.setDirectMode(true);
            }

            if (!this.allTables) {
                if (in.hasOption("table")) {
                    out.setTableName(in.getOptionValue("table"));
                }

                if (in.hasOption("columns")) {
                    String[] cols = in.getOptionValue("columns").split(",");

                    for(int i = 0; i < cols.length; ++i) {
                        cols[i] = cols[i].trim();
                    }

                    out.setColumns(cols);
                }

                if (in.hasOption("split-by")) {
                    out.setSplitByCol(in.getOptionValue("split-by"));
                }

                if (in.hasOption("split-limit")) {
                    out.setSplitLimit(Integer.parseInt(in.getOptionValue("split-limit")));
                }

                if (in.hasOption("where")) {
                    out.setWhereClause(in.getOptionValue("where"));
                }

                if (in.hasOption("target-dir")) {
                    out.setTargetDir(in.getOptionValue("target-dir"));
                }

                if (in.hasOption("append")) {
                    out.setAppendMode(true);
                }

                if (in.hasOption("delete-target-dir")) {
                    out.setDeleteMode(true);
                }

                if (in.hasOption("query")) {
                    out.setSqlQuery(in.getOptionValue("query"));
                }

                if (in.hasOption("boundary-query")) {
                    out.setBoundaryQuery(in.getOptionValue("boundary-query"));
                }

                if (in.hasOption("merge-key")) {
                    out.setMergeKeyCol(in.getOptionValue("merge-key"));
                }

                this.applyValidationOptions(in, out);
            }

            if (in.hasOption("warehouse-dir")) {
                out.setWarehouseDir(in.getOptionValue("warehouse-dir"));
            }

            if (in.hasOption("as-sequencefile")) {
                out.setFileLayout(SqoopOptions.FileLayout.SequenceFile);
            }

            if (in.hasOption("as-textfile")) {
                out.setFileLayout(SqoopOptions.FileLayout.TextFile);
            }

            if (in.hasOption("as-avrodatafile")) {
                out.setFileLayout(SqoopOptions.FileLayout.AvroDataFile);
            }

            if (in.hasOption("as-parquetfile")) {
                out.setFileLayout(SqoopOptions.FileLayout.ParquetFile);
            }

            if (in.hasOption("num-mappers")) {
                out.setNumMappers(Integer.parseInt(in.getOptionValue("num-mappers")));
            }

            if (in.hasOption("mapreduce-job-name")) {
                out.setMapreduceJobName(in.getOptionValue("mapreduce-job-name"));
            }

            if (in.hasOption("compress")) {
                out.setUseCompression(true);
            }

            if (in.hasOption("compression-codec")) {
                out.setCompressionCodec(in.getOptionValue("compression-codec"));
            }

            if (in.hasOption("direct-split-size")) {
                out.setDirectSplitSize(Long.parseLong(in.getOptionValue("direct-split-size")));
            }

            if (in.hasOption("inline-lob-limit")) {
                out.setInlineLobLimit(Long.parseLong(in.getOptionValue("inline-lob-limit")));
            }

            if (in.hasOption("fetch-size")) {
                out.setFetchSize(new Integer(in.getOptionValue("fetch-size")));
            }

            if (in.hasOption("jar-file")) {
                out.setExistingJarName(in.getOptionValue("jar-file"));
            }

            if (in.hasOption("autoreset-to-one-mapper")) {
                out.setAutoResetToOneMapper(true);
            }

            if (in.hasOption("escape-mapping-column-names")) {
                out.setEscapeMappingColumnNamesEnabled(Boolean.parseBoolean(in.getOptionValue("escape-mapping-column-names")));
            }

            this.applyIncrementalOptions(in, out);
            this.applyHiveOptions(in, out);
            this.applyOutputFormatOptions(in, out);
            this.applyInputFormatOptions(in, out);
            this.applyCodeGenOptions(in, out, this.allTables);
            this.applyHBaseOptions(in, out);
            this.applyHCatalogOptions(in, out);
            this.applyAccumuloOptions(in, out);
        } catch (NumberFormatException var5) {
            throw new SqoopOptions.InvalidOptionsException("Error: expected numeric argument.\nTry --help for usage.");
        }
    }

    protected void validateImportOptions(SqoopOptions options) throws SqoopOptions.InvalidOptionsException {
        if (!this.allTables && options.getTableName() == null && options.getSqlQuery() == null) {
            throw new SqoopOptions.InvalidOptionsException("--table or --query is required for import. (Or use sqoop import-all-tables.)\nTry --help for usage instructions.");
        } else if (options.getExistingJarName() != null && options.getClassName() == null) {
            throw new SqoopOptions.InvalidOptionsException("Jar specified with --jar-file, but no class specified with --class-name.\nTry --help for usage instructions.");
        } else if (options.getTargetDir() != null && options.getWarehouseDir() != null) {
            throw new SqoopOptions.InvalidOptionsException("--target-dir with --warehouse-dir are incompatible options.\nTry --help for usage instructions.");
        } else if (options.getTableName() != null && options.getSqlQuery() != null) {
            throw new SqoopOptions.InvalidOptionsException("Cannot specify --query and --table together.\nTry --help for usage instructions.");
        } else if (options.getSqlQuery() != null && options.getTargetDir() == null && options.getHBaseTable() == null && options.getHCatTableName() == null && options.getAccumuloTable() == null) {
            throw new SqoopOptions.InvalidOptionsException("Must specify destination with --target-dir. \nTry --help for usage instructions.");
        } else if (options.getSqlQuery() != null && options.doHiveImport() && options.getHiveTableName() == null) {
            throw new SqoopOptions.InvalidOptionsException("When importing a query to Hive, you must specify --hive-table.\nTry --help for usage instructions.");
        } else if (options.getSqlQuery() != null && options.getNumMappers() > 1 && options.getSplitByCol() == null) {
            throw new SqoopOptions.InvalidOptionsException("When importing query results in parallel, you must specify --split-by.\nTry --help for usage instructions.");
        } else {
            if (options.isDirect()) {
                this.validateDirectImportOptions(options);
            } else {
                if (this.allTables && options.isValidationEnabled()) {
                    throw new SqoopOptions.InvalidOptionsException("Validation is not supported for all tables but single table only.");
                }

                if (options.getSqlQuery() != null && options.isValidationEnabled()) {
                    throw new SqoopOptions.InvalidOptionsException("Validation is not supported for free from query but single table only.");
                }

                if (options.getWhereClause() != null && options.isValidationEnabled()) {
                    throw new SqoopOptions.InvalidOptionsException("Validation is not supported for where clause but single table only.");
                }

                if (options.getIncrementalMode() != SqoopOptions.IncrementalMode.None && options.isValidationEnabled()) {
                    throw new SqoopOptions.InvalidOptionsException("Validation is not supported for incremental imports but single table only.");
                }

                if ((options.getTargetDir() != null || options.getWarehouseDir() != null) && options.getHCatTableName() != null) {
                    throw new SqoopOptions.InvalidOptionsException("--hcatalog-table cannot be used  --warehouse-dir or --target-dir options");
                }

                if (options.isDeleteMode() && options.isAppendMode()) {
                    throw new SqoopOptions.InvalidOptionsException("--append and --delete-target-dir can not be used together.");
                }

                if (options.isDeleteMode() && options.getIncrementalMode() != SqoopOptions.IncrementalMode.None) {
                    throw new SqoopOptions.InvalidOptionsException("--delete-target-dir can not be used with incremental imports.");
                }

                if (options.getAutoResetToOneMapper() && options.getSplitByCol() != null) {
                    throw new SqoopOptions.InvalidOptionsException("--autoreset-to-one-mapper and --split-by cannot be used together.");
                }
            }

        }
    }

    void validateDirectImportOptions(SqoopOptions options) throws SqoopOptions.InvalidOptionsException {
        this.validateDirectMysqlOptions(options);
        this.validateDirectDropHiveDelimOption(options);
//        this.validateHasDirectConnectorOption(options);
    }

    void validateDirectDropHiveDelimOption(SqoopOptions options) throws SqoopOptions.InvalidOptionsException {
        if (options.doHiveDropDelims()) {
            throw new SqoopOptions.InvalidOptionsException("Direct import currently do not support dropping hive delimiters, please remove parameter --hive-drop-import-delims.");
        }
    }

    void validateDirectMysqlOptions(SqoopOptions options) throws SqoopOptions.InvalidOptionsException {
        if (options.getFileLayout() != SqoopOptions.FileLayout.TextFile && SupportedManagers.MYSQL.isTheManagerTypeOf(options)) {
            throw new SqoopOptions.InvalidOptionsException("MySQL direct import currently supports only text output format. Parameters --as-sequencefile --as-avrodatafile and --as-parquetfile are not supported with --direct params in MySQL case.");
        }
    }

    private void validateIncrementalOptions(SqoopOptions options) throws SqoopOptions.InvalidOptionsException {
        if (options.getIncrementalMode() != SqoopOptions.IncrementalMode.None && options.getIncrementalTestColumn() == null) {
            throw new SqoopOptions.InvalidOptionsException("For an incremental import, the check column must be specified with --check-column. \nTry --help for usage instructions.");
        } else if (options.getIncrementalMode() == SqoopOptions.IncrementalMode.None && options.getIncrementalTestColumn() != null) {
            throw new SqoopOptions.InvalidOptionsException("You must specify an incremental import mode with --incremental. \nTry --help for usage instructions.");
        } else if (options.getIncrementalMode() == SqoopOptions.IncrementalMode.DateLastModified && options.getFileLayout() == SqoopOptions.FileLayout.AvroDataFile) {
            throw new SqoopOptions.InvalidOptionsException("--incremental lastmodified cannot be used in conjunction with --as-avrodatafile.\nTry --help for usage instructions.");
        }
    }

    public void validateOptions(SqoopOptions options) throws SqoopOptions.InvalidOptionsException {
        options.setExtraArgs(this.getSubcommandArgs(this.extraArguments));
        int dashPos = this.getDashPosition(this.extraArguments);
        if (this.hasUnrecognizedArgs(this.extraArguments, 0, dashPos)) {
            throw new SqoopOptions.InvalidOptionsException("\nTry --help for usage instructions.");
        } else {
            this.validateImportOptions(options);
            this.validateIncrementalOptions(options);
            this.validateCommonOptions(options);
            this.validateCodeGenOptions(options);
            this.validateOutputFormatOptions(options);
            this.validateHBaseOptions(options);
            this.validateHiveOptions(options);
            this.validateHCatalogOptions(options);
            this.validateAccumuloOptions(options);
        }
    }


}