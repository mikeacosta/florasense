package io.florasense.streams;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.florasense.streams.models.SensorDataProps;

import java.nio.ByteBuffer;

public class StreamWriter {

    private KinesisProducer producer;

    public StreamWriter(KinesisProducer producer) {
        this.producer = producer;
    }

    public void putRecord( byte[] bytes) {

        final String partitionKey = "1";

        ListenableFuture<UserRecordResult> future = producer.addUserRecord(
                SensorDataProps.getKinesisStreamName(),
                partitionKey,
                ByteBuffer.wrap(bytes)
        );

        Futures.addCallback(future, new FutureCallback<UserRecordResult>() {

            public void onSuccess(UserRecordResult result) {
                System.out.println("sequence no: " + result.getSequenceNumber());
            }

            public void onFailure(Throwable t) {
                if (t instanceof UserRecordFailedException) {
                    UserRecordFailedException e = (UserRecordFailedException) t;
                    UserRecordResult result = e.getResult();

                    Attempt last = Iterables.getLast(result.getAttempts());
                    System.err.println(String.format("Put failed - %s", last.getErrorMessage()));
                }
            }
        });
    }
}
