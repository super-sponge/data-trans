package com.sponge.etl.jobxml;

import com.google.gson.JsonObject;

public class JobSettingXml extends BaseXml {
    private int channel = 3;

    public JsonObject jsonTrans() {
        JsonObject jsonSpeedObject = new JsonObject();
        jsonSpeedObject.addProperty("channel", channel);

        JsonObject jsonObject = new JsonObject();
        jsonObject.add("speed", jsonSpeedObject);

        return jsonObject;
    }

    public int getChannel() {
        return channel;
    }

    public void setChannel(int channel) {
        this.channel = channel;
    }
}
