package io.florasense.streams;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;
import com.pubnub.api.models.consumer.pubsub.PNSignalResult;
import com.pubnub.api.models.consumer.pubsub.objects.PNMembershipResult;
import com.pubnub.api.models.consumer.pubsub.objects.PNSpaceResult;
import com.pubnub.api.models.consumer.pubsub.objects.PNUserResult;
import io.florasense.streams.models.SensorDataProps;
import io.florasense.streams.utils.SensorHelper;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class StreamSubscriber {

    public static void main(String[] args) {
        PNConfiguration pnConfiguration = new PNConfiguration();
        pnConfiguration.setSubscribeKey(SensorDataProps.getSubscribeKey());
        subscribe(pnConfiguration);
    }

    private static void subscribe(PNConfiguration pnConfiguration) {

        final PubNub pubnub = new PubNub(pnConfiguration);
        final KinesisProducer producer = createKinesisProducer();
        final StreamWriter writer = new StreamWriter(producer);

        pubnub.addListener(new SubscribeCallback() {

            @Override
            public void status(PubNub pubnub, PNStatus status) {
                PNStatusCategory category = status.getCategory();
                System.out.println("category: " + category.name());
            }

            @Override
            public void message(PubNub pubnub, PNMessageResult result) {
                try {
                    String message = SensorHelper.getSensorMessage(result.getMessage());
                    System.out.println("Received message content: " + message);
                    byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
                    writer.putRecord(msgBytes);
                } catch (Exception e) {
                    System.out.println("Exception: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            @Override
            public void presence(PubNub pubnub, PNPresenceEventResult presence){

            }

            public void signal(PubNub pubNub, PNSignalResult pnSignalResult) {

            }

            public void user(PubNub pubNub, PNUserResult pnUserResult) {

            }

            public void space(PubNub pubNub, PNSpaceResult pnSpaceResult) {

            }

            public void membership(PubNub pubNub, PNMembershipResult pnMembershipResult) {

            }
        });

        pubnub.subscribe().channels(Collections.singletonList(SensorDataProps.getChannelName())).execute();
    }

    private static KinesisProducer createKinesisProducer() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setRequestTimeout(60000)
                .setRecordMaxBufferedTime(15000)
                .setRegion("us-west-2");
        return new KinesisProducer(config);
    }
}
