package com.sponge.etl.jobxml;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.cli.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class JobXml {

    private String oraJdbcUrl;
    private String oraUsername;
    private String oraPassword;

    private String hiveDbPath = "/apps/hive/warehouse";
    private String defaultFS;

    private List<String> metaInfos = null;

    private String jobJsonDistPath = "./";

    private JobSettingXml jobSettingXml = new JobSettingXml();


    public void oraToHdfs(String where,
                          String writeMode,
                          String fileType,
                          String compress,
                          String fieldDelimiter
    ) {
        List<String> oraCols = new ArrayList<String>(32);
        List<JsonObject> hdfsCols = new ArrayList<JsonObject>(32);

        HdfsWriterXml jsonwriter = new HdfsWriterXml(this.getDefaultFS(), this.getHiveDbPath(), null);
        jsonwriter.setColumn(hdfsCols);

        if (writeMode != null) {
            jsonwriter.setWriteMode(writeMode);
        }
        if (fileType != null) {
            jsonwriter.setFileType(fileType);
        }
        if (compress != null) {
            jsonwriter.setCompress(compress);
        }
        if (fieldDelimiter != null) {
            jsonwriter.setFieldDelimiter(fieldDelimiter);
        }

        OracleReaderXml jsonreader = new OracleReaderXml(this.getOraJdbcUrl(),
                this.getOraUsername(),
                this.getOraPassword(),
                null,
                null,
                where);
        if (where != null) {
            jsonreader.setWhere(where);
        }
        jsonreader.setColumns(oraCols);

        String preTable = null;
        String preSchemaName = null;
        for (String line : this.metaInfos) {
            String[] splits = line.split(",");
            String schemaName = splits[0].toLowerCase();
            String tableName = splits[1].toLowerCase();
            String column = splits[2];
            //hive data type
            String columnType = splits[4];

            if (! tableName.equals(preTable)) {
                //ignore first
                if (oraCols.size() != 0) {
                    jsonreader.setTablename(preSchemaName + "." + preTable);
                    jsonwriter.setPath(this.getHiveTablePath(preTable));

                    //create jobXml
                    String jobXmlPath = this.getJobJsonDistPath() + "job_" + preTable + ".json";
                    String jobJsonResult = this.createOraTohdfsJson(jsonreader, jsonwriter);
                    JobUtils.writeStrToFile(jobXmlPath, jobJsonResult);

                    //create table sql
                    String tableCreateSqlPath = this.getJobJsonDistPath() + "job_" + preTable + ".sql";
                    String hiveScriptResult = this.createHiveScript(preTable, jsonwriter);
                    JobUtils.writeStrToFile(tableCreateSqlPath, hiveScriptResult);

                    oraCols.clear();
                    hdfsCols.clear();
                }
                preTable = tableName;
                preSchemaName = schemaName;
            }

            JsonObject hdfsColItem = new JsonObject();
            hdfsColItem.addProperty("name", column);
            hdfsColItem.addProperty("type", columnType);
            hdfsCols.add(hdfsColItem);
            oraCols.add(column);

        }

        jsonreader.setTablename(preSchemaName + "." + preTable);
        jsonwriter.setPath(this.getHiveTablePath(preTable));

        //create jobXml
        String jobXmlPath = this.getJobJsonDistPath() + "job_" + preTable + ".json";
        String jobJsonResult = this.createOraTohdfsJson(jsonreader, jsonwriter);
        JobUtils.writeStrToFile(jobXmlPath, jobJsonResult);

        //create table sql
        String tableCreateSqlPath = this.getJobJsonDistPath() + "job_" + preTable + ".sql";
        String hiveScriptResult = this.createHiveScript(preTable, jsonwriter);
        JobUtils.writeStrToFile(tableCreateSqlPath, hiveScriptResult);


    }

    private String createOraTohdfsJson(OracleReaderXml oraReader,
                                 HdfsWriterXml hdfsWriter) {


        JsonObject jsonJobObject = new JsonObject();
        jsonJobObject.add("setting", jobSettingXml.jsonTrans());
        JsonArray jsonContent = new JsonArray();
        jsonJobObject.add("content", jsonContent);
        JsonObject jsonContentItem = new JsonObject();
        jsonContent.add(jsonContentItem);
        jsonContentItem.add("reader", oraReader.jsonTrans());
        jsonContentItem.add("writer", hdfsWriter.jsonTrans());

        JsonObject jobResult = new JsonObject();
        jobResult.add("job", jsonJobObject);

        return JobUtils.jsonFormatter(jobResult.toString());
    }

    /**
     * the script for create hive table
     * @param tableName
     * @param hdfsWriter
     * @return
     *
     * create table tableName ( .. )
     * row format delimited
     * stored as orc
     * location '/apps/hive/warehouse/tableName'
     */

    private String createHiveScript(String tableName,
                                    HdfsWriterXml hdfsWriter
                                    ) {
        String tableFilePath = hdfsWriter.getPath();
        String sql = "create table " + tableName + "(\n";
        Boolean firstFlag = false;
        for(JsonObject js : hdfsWriter.getColumn()) {
            String colName = js.get("name").getAsString();
            String colType = js.get("type").getAsString();
            if (firstFlag) {
                sql += ",";
            }
            sql += colName + " " + colType + "\n";
            firstFlag = true;
        }
        sql += ")\nrow format delimited\n";
        sql += "stored as " + hdfsWriter.getFileType() + "\n";
        sql += "location '" + hdfsWriter.getPath() + "';\n";

        return  sql;
    }

    public void initOraEnv(String jdbcUrl,
                           String userName,
                           String password) {
        this.setOraJdbcUrl(jdbcUrl);
        this.setOraUsername(userName);
        this.setOraPassword(password);
    }

    /**
     * @param metaFilePath meta file path.
     *                     rows like
     */

    public void initMetaInfoEnv(String metaFilePath) {
        if (metaInfos == null) {
            metaInfos = new ArrayList<String>();
        } else {
            metaInfos.clear();
        }

        try {
            File file = new File(metaFilePath);
            BufferedReader bufread;
            String line;
            bufread = new BufferedReader(new FileReader(file));
            while ((line = bufread.readLine()) != null) {
                metaInfos.add(line);
            }
            bufread.close();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void initHdfsEnv(String defaultFS) {
        this.setDefaultFS(defaultFS);
    }

    public void initHdfsEnv(String defaultFS, String hiveDbPath) {
        this.setDefaultFS(defaultFS);
        this.setHiveDbPath(hiveDbPath);
    }

    public void initJobSetting(int channel) {
        this.jobSettingXml.setChannel(channel);
    }

    public String getOraJdbcUrl() {
        return oraJdbcUrl;
    }

    public void setOraJdbcUrl(String oraJdbcUrl) {
        this.oraJdbcUrl = oraJdbcUrl;
    }

    public String getOraUsername() {
        return oraUsername;
    }

    public void setOraUsername(String oraUsername) {
        this.oraUsername = oraUsername;
    }

    public String getOraPassword() {
        return oraPassword;
    }

    public void setOraPassword(String oraPassword) {
        this.oraPassword = oraPassword;
    }

    public String getHiveDbPath() {
        return hiveDbPath;
    }

    public void setHiveDbPath(String hiveDbPath) {
        this.hiveDbPath = hiveDbPath;
    }

    public String getDefaultFS() {
        return defaultFS;
    }

    public void setDefaultFS(String defaultFS) {
        this.defaultFS = defaultFS;
    }

    public String getJobJsonDistPath() {
        return jobJsonDistPath.endsWith("/") ? jobJsonDistPath: jobJsonDistPath + "/";
    }

    public void setJobJsonDistPath(String jobJsonDistPath) {
        this.jobJsonDistPath = jobJsonDistPath;
    }

    public String getHiveTablePath(String tableName) {
        if (this.getHiveDbPath().endsWith("/")) {
            return this.getHiveDbPath() + tableName;
        } else {
            return this.getHiveDbPath() + "/" + tableName;
        }
    }


    public static void ExecuteCommandLine(String[] args ) throws IOException {
        Options opts = new Options();
        opts.addOption("h", false, "Help description");
        opts.addOption("j", true, "jdbc connect string");
        opts.addOption("u", true, "user");
        opts.addOption("p", true, "password");
        opts.addOption("m", true, "meta info file");
        opts.addOption("f", true, "hdfs defaultFS hdfs://localhost:8020 ");
        opts.addOption("e", true, "hive databases path default[/apps/hive/warehouse]");
        opts.addOption("w", true, "writeMode type append|nonConflict default[orc]");
        opts.addOption("t", true, "fileType orc|text default[orc]");
        opts.addOption("c", true, "compress type NONE|SNAPPY default[NONE]");
        opts.addOption("d", true, "delimiter default[\\t]");
        opts.addOption("n", true, "channel num default[2]");
        opts.addOption("o", true, "output json path");


        CommandLineParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(opts, args);
            if (cl.getOptions().length > 0) {
                if (cl.hasOption('h')) {
                    HelpFormatter hf = new HelpFormatter();
                    hf.printHelp("May Options", opts);
                } else {
                    String url = cl.getOptionValue("j");
                    String user = cl.getOptionValue("u");
                    String password = cl.getOptionValue("p");
                    String metaFilePath = cl.getOptionValue("m");
                    String defaultFsIn= cl.getOptionValue("f");
                    String hiveDbPath= cl.getOptionValue("e", "/apps/hive/warehouse");
                    String writeMode = cl.getOptionValue("w", "append");
                    String fileType = cl.getOptionValue("t", "orc");
                    String compress = cl.getOptionValue("c", "NONE");
                    String delimter = cl.getOptionValue("d", "\t");
                    int channels = Integer.parseInt(cl.getOptionValue("n", "2"));
                    String outJsonPath = cl.getOptionValue("o","./");


                    if (url ==null || user == null || password == null
                            || outJsonPath == null || defaultFsIn == null
                            || metaFilePath == null) {
                        HelpFormatter hf = new HelpFormatter();
                        hf.printHelp("May Options", opts);
                        return;
                    }

                    JobXml jobXml = new JobXml();

                    jobXml.initJobSetting(channels);
                    jobXml.initOraEnv(url, user, password);
                    jobXml.initHdfsEnv(defaultFsIn, hiveDbPath);
                    jobXml.initMetaInfoEnv(metaFilePath);
                    jobXml.setJobJsonDistPath(outJsonPath);
                    jobXml.oraToHdfs("",writeMode,fileType,compress,delimter);

                }
            } else {
                System.err.println("ERROR_NOARGS");
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {
        ExecuteCommandLine(args);
    }
}
