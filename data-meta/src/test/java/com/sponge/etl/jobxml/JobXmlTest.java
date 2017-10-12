package com.sponge.etl.jobxml;

import com.google.gson.JsonObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class JobXmlTest {
    @Test
    public void oraToHdfs() throws Exception {
        JobXml jobXml = new JobXml();

        jobXml.initJobSetting(5);
        jobXml.initOraEnv("jdbc:oracle:thin:@10.0.8.156:1521:orcl",
                "scott",
                "tiger");
        jobXml.initHdfsEnv("hdfs://sdc1.sefon.com:8020");
        jobXml.initMetaInfoEnv("./data/meta.txt");
        jobXml.setJobJsonDistPath("./data/");
        jobXml.oraToHdfs("", "append","orc","NONE",null);

    }

    @Test
    public void ExecuteCommandLine() throws Exception {
        String[] args = {"",
                "-j", "jdbc:oracle:thin:@10.0.8.156:1521:orcl",
                "-u", "scott",
                "-p", "tiger",
                "-m", "./data/meta.txt",
                "-f", "hdfs://sdc1.sefon.com:8020",
                "-o", "./data"
        };
        JobXml.ExecuteCommandLine(args);
    }

}