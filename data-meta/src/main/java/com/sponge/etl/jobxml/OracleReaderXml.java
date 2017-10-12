package com.sponge.etl.jobxml;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class OracleReaderXml extends BaseXml {
    private final String READER_NAME = "oraclereader";
    private String jdbcUrl;
    private String username;
    private String password;
    private String tablename;
    private List<String> columns;
    private String where;


    public JsonObject jsonTrans() {
        JsonObject jsonReader = new JsonObject();
        JsonObject jsonParameter = new JsonObject();
        JsonArray jsonColumn = new JsonArray();
        JsonArray jsonConnection = new JsonArray();
        JsonObject jsonConnectionItem = new JsonObject();
        JsonArray jsonTable = new JsonArray();
        JsonArray jsonJdbcUrl = new JsonArray();


        jsonReader.addProperty("name", READER_NAME);

        jsonReader.add("parameter", jsonParameter);
        jsonParameter.addProperty("username", username);
        jsonParameter.addProperty("password", password);
        jsonParameter.addProperty("where", where);

        jsonParameter.add("column", jsonColumn);
        for(String col : columns) {
            jsonColumn.add(col);
        }

        jsonParameter.add("connection", jsonConnection);
        jsonConnection.add(jsonConnectionItem);
        jsonConnectionItem.add("table", jsonTable);
        jsonConnectionItem.add("jdbcUrl", jsonJdbcUrl);

        jsonTable.add(tablename);
        jsonJdbcUrl.add(jdbcUrl);


        return  jsonReader;
    }

    public OracleReaderXml() {
        this.columns = new ArrayList<String>();
    }

    public OracleReaderXml(String jdbcUrl,
                           String username,
                           String password,
                           String tablename,
                           List<String> columns,
                           String where) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.tablename = tablename;
        this.columns = columns;
        this.where = where;
    }

    public String getREADER_NAME() {
        return READER_NAME;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
