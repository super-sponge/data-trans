package com.sponge.etl.jobxml;

import com.google.gson.JsonObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class OracleReaderXmlTest {
    @Test
    public void readerJson() throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@10.0.8.156:1521:orcl";
        String username = "scott";
        String password = "tiger";
        String tablename = "vote_record";
        String where = "";
        List<String> lstColumns = new ArrayList<String>();
        lstColumns.add("id");
        lstColumns.add("user_id");
        lstColumns.add("vote_id");
        lstColumns.add("group_id");
        lstColumns.add("create_time");

        OracleReaderXml oracleReaderXml= new OracleReaderXml(jdbcUrl, username, password, tablename, lstColumns, where);
        JsonObject jsonObject = oracleReaderXml.jsonTrans();
        System.out.println(JobUtils.jsonFormatter(jsonObject.toString()));
    }

}