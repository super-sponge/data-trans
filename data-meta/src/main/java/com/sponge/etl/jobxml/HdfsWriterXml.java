package com.sponge.etl.jobxml;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.List;

public class HdfsWriterXml extends BaseXml{

    private final String WRITER_NAME = "hdfswriter";
    private String defaultFS;
    private String fileType = "orc";
    private String path;
    private String fileName= "datax";
    private List<JsonObject> column ;
    private String writeMode = "append";
    private String fieldDelimiter = "\t";
    private String compress = "NONE";


    public HdfsWriterXml(String defaultFS, String path, List<JsonObject> column) {
        this.defaultFS = defaultFS;
        this.path = path;
        this.column = column;
    }

    public JsonObject jsonTrans() {
        JsonObject jsonWriter = new JsonObject();
        jsonWriter.addProperty("name", WRITER_NAME);

        JsonObject jsonParamter = new JsonObject();
        jsonWriter.add("parameter", jsonParamter);
        jsonParamter.addProperty("defaultFS", defaultFS);
        jsonParamter.addProperty("fileType", fileType);
        jsonParamter.addProperty("path", path);
        jsonParamter.addProperty("fileName", fileName);

        JsonArray jsonColumns = new JsonArray();
        for(JsonObject col: column) {
            jsonColumns.add(col);
        }
        jsonParamter.add("column", jsonColumns);
        jsonParamter.addProperty("writeMode", writeMode);
        jsonParamter.addProperty("fieldDelimiter", fieldDelimiter);
        jsonParamter.addProperty("compress", compress);

        return jsonWriter;
    }

    public String getWRITER_NAME() {
        return WRITER_NAME;
    }

    public String getDefaultFS() {
        return defaultFS;
    }

    public void setDefaultFS(String defaultFS) {
        this.defaultFS = defaultFS;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public List<JsonObject> getColumn() {
        return column;
    }

    public void setColumn(List<JsonObject> column) {
        this.column = column;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }
}
