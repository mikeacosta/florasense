package io.florasense.streams.models;

public class SensorDataProps {

    public static String getChannelName() {
        return "pubnub-sensor-network";
    }

    public static String getSubscribeKey() {
        return "sub-c-5f1b7c8e-fbee-11e3-aa40-02ee2ddab7fe";
    }

    public static String getKinesisStreamName() {
        return "sensor-network-stream";
    }
}
