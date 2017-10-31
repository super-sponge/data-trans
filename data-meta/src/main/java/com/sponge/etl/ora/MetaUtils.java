package com.sponge.etl.ora;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaUtils {
    private static final Logger LOG = LoggerFactory
            .getLogger(MetaUtils.class);

    private static final String HIVE_DEFAULT_COLUMN_TYPE="STRING";
    private static final Map<String, String> COLUMNS_MAPS = new HashMap<String, String>(){{
        put("LONG","STRING");
        put("CHAR","STRING");
        put("NCHAR","STRING");
        put("VARCHAR","STRING");
        put("VARCHAR2","STRING");
        put("NVARCHAR2","STRING");
        put("CLOB","STRING");
        put("NCLOB","STRING");
        put("CHARACTER","STRING");
        put("CHARACTER VARYING","STRING");
        put("CHAR VARYING","STRING");
        put("NATIONAL CHARACTER","STRING");
        put("NATIONAL CHAR","STRING");
        put("NATIONAL CHARACTER VARYING","STRING");
        put("NATIONAL CHAR","STRING");
        put("NCHAR VARYING","STRING");
        put("NUMERIC","DOUBLE");
        put("DECIMAL","DOUBLE");
        put("FLOAT","DOUBLE");
        put("DOUBLE PRECISION","DOUBLE");
        put("REAL","DOUBLE");
        put("NUMBER","BIGINT");
        put("INTEGER","INT");
        put("INT","INT");
        put("SMALLINT","SMALLINT");
        put("TIMESTAMP","TIMESTAMP");
        put("TIMESTAMP(6)","TIMESTAMP");
        put("DATE","DATE");
        put("BOOL","BOOLEAN");
    }};

    public static class MetaInfo{
        String schemaName;
        String tableName;
        String columnName;
        String columnType;

        public MetaInfo() {
        }

        public MetaInfo(String schemaName, String tableName, String columnName, String columnType) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.columnName = columnName;
            this.columnType = columnType;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
        }

        @Override
        public String toString() {
            return "MetaInfo{" +
                    "tableName='" + tableName + '\'' +
                    ", columnName='" + columnName + '\'' +
                    ", columnType='" + columnType + '\'' +
                    '}';
        }
    }

    public static  String transOracleColumnToHiveColumn(String  oracleCol) {
        String result = COLUMNS_MAPS.get(oracleCol);
        if (null == result) {
            LOG.warn("Oracle type [" + oracleCol + "] can't cast to hive type default " + HIVE_DEFAULT_COLUMN_TYPE);
            return  HIVE_DEFAULT_COLUMN_TYPE;
        } else {
            return  result;
        }
    }


    public static List<MetaInfo> getOracleTableMetaInfo(String url, String user, String password, String tableName) {
       return getOracleTableMetaInfo(url,user,password,tableName,user);
    }

    /**
     *
     * @param url  jdbc:oracle:thin:@10.0.8.156:1521:orcl
     * @param user  user name
     * @param password password
     * @param tableName  tableName
     * @param schemaName schemaName
     * @return
     */
    public static List<MetaInfo> getOracleTableMetaInfo(String url, String user, String password, String tableName, String schemaName) {
        List<MetaInfo> lstMetainfo = new ArrayList<MetaInfo>(64);

        Connection con = null;
        PreparedStatement pre = null;
        ResultSet result = null;
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            LOG.info("try connecting database！");
            con = DriverManager.getConnection(url, user, password);
            LOG.info("connect succed!");
            String sql = null;
            if ("".equals(tableName) || null == tableName) {
                sql = "select owner,table_name,column_name,data_type from all_tab_columns  " +
                        "where owner = ? order by table_name,column_id";
                pre = con.prepareStatement(sql);
                pre.setString(1, schemaName.toUpperCase());
            } else {
                sql = "select  owner,table_name,column_name,data_type from all_tab_columns " +
                        "where owner = ? and table_name = ? order by owner,table_name,column_id";
                pre = con.prepareStatement(sql);
                pre.setString(1, schemaName.toUpperCase());
                pre.setString(2, tableName.toUpperCase());
            }
            result = pre.executeQuery();
            while (result.next())
                lstMetainfo.add(new MetaInfo(result.getString("owner"),
                        result.getString("table_name"),
                        result.getString("column_name"),
                        result.getString("data_type"))
                );
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if (result != null)
                    result.close();
                if (pre != null)
                    pre.close();
                if (con != null)
                    con.close();
                LOG.debug("database closed");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        return lstMetainfo;
    }

    private static void writeMetainfoToFile(List<MetaInfo> lstMetainfo, String filePath) throws IOException {
        FileWriter out = null;
        out = new FileWriter(new File(filePath));
        for (MetaInfo metaInfo : lstMetainfo) {
            out.write(metaInfo.getSchemaName() + ","
                    + metaInfo.getTableName() + ","
                    + metaInfo.getColumnName() + ","
                    + metaInfo.getColumnType() + ","
                    + transOracleColumnToHiveColumn(metaInfo.getColumnType()) +"\n");
        }
        out.close();
    }

    public static void ExecuteCommandLine(String[] args ) throws IOException {
        Options opts = new Options();
        opts.addOption("h", false, "Help description");
        opts.addOption("j", true, "jdbc connect string");
        opts.addOption("u", true, "user");
        opts.addOption("p", true, "password");
        opts.addOption("t", true, "table name");
        opts.addOption("s", true, "table name");
        opts.addOption("f", true, "output json path");


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
                    String tableName = cl.getOptionValue("t");
                    String outJsonPath = cl.getOptionValue("f");
                    String schemaName = cl.getOptionValue("s", user);

                    if (url ==null || user == null || password == null || outJsonPath == null) {
                        HelpFormatter hf = new HelpFormatter();
                        hf.printHelp("May Options", opts);
                        return;
                    }

                    List<MetaInfo> lstMetaInfo = getOracleTableMetaInfo(url, user, password, tableName,schemaName);
                    writeMetainfoToFile(lstMetaInfo, outJsonPath);
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
