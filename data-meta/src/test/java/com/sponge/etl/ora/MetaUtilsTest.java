package com.sponge.etl.ora;

import org.junit.Test;

import static org.junit.Assert.*;

public class MetaUtilsTest {
    @Test
    public void executeCommandLine() throws Exception {
        String[] args = {"",
                "-j", "jdbc:oracle:thin:@10.0.8.156:1521:orcl",
                "-u", "scott",
                "-p", "tiger",
                "-t", "vote_record",
                "-f", "./data/vote_record.txt"
        };
        MetaUtils.ExecuteCommandLine(args);

        String[] args2 = {"",
                "-j", "jdbc:oracle:thin:@10.0.8.156:1521:orcl",
                "-u", "scott",
                "-p", "tiger",
                "-f", "./data/all.txt"
        };
        MetaUtils.ExecuteCommandLine(args2);
    }

}