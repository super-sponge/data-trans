package com.sponge.etl.jobxml;

import com.google.gson.JsonObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class HdfsWriterXmlTest {
    @Test
    public void jsonTrans() throws Exception {
        String defaults = "hdfs://sdc1.sefon.com:8020";
        String path = "/apps/hive/warehouse/emp";
        List<JsonObject> column = new ArrayList<JsonObject>();
        JsonObject item = new JsonObject();
        item.addProperty("name","id");
        item.addProperty("type","INT");
        column.add(item);
        item = new JsonObject();
        item.addProperty("name", "user_name");
        item.addProperty("type", "STRING");
        column.add(item);

        HdfsWriterXml hdfsWriterXml = new HdfsWriterXml(defaults, path, column);
        JsonObject jsonObject = hdfsWriterXml.jsonTrans();
        System.out.println(JobUtils.jsonFormatter(jsonObject.toString()));
    }

}