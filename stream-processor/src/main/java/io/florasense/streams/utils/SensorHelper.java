package io.florasense.streams.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SensorHelper {

    public static String getSensorMessage(JsonElement element) {

        JsonObject jsonObject = element.getAsJsonObject();

        String sensorUuid = jsonObject.get("sensor_uuid").toString().replaceAll("\"", "");
        int sensorNo = getSensorNumber(1, 75);

        jsonObject.addProperty("sensor_number", sensorNo);
        jsonObject.addProperty("reading_id", getReadingId(sensorUuid, sensorNo));
        jsonObject.remove("sensor_uuid");
        jsonObject.remove("radiation_level");
        return jsonObject.toString();
    }

    public static int getSensorNumber(int min, int max) {
        return min + (int)(Math.random() * ((max - 1) + 1));
    }

    private static String getReadingId(String uuid, int sensorNo) {
        return uuid + "-" + sensorNo + getSensorNumber(100, 999);
    }
}
