package com.intuit.kcl.example.kinesis;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.KinesisClientUtil;

//import software.amazon.awssdk.core.SdkBytes;
//import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

public class SampleProducer {
    private KinesisAsyncClient kinesisClient;

//    public static void main1(String[] args) {
//        String myStreamName = "naga-test";
//        String sequenceNumberOfPreviousRecord = "0";
//
//        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
//
//        clientBuilder.setRegion("us-west-2");
//        clientBuilder.setCredentials(new ProfileCredentialsProvider("default"));
//
//        AmazonKinesis client = clientBuilder.build();
//
//        for (int j = 0; j < 10; j++)
//        {
////            PutRecordRequest putRecordRequest = new PutRecordRequest();
////            putRecordRequest.setStreamName( myStreamName );
////            putRecordRequest.setData(ByteBuffer.wrap( String.format( "testData-%d", j ).getBytes() ));
////            putRecordRequest.setPartitionKey( String.format( "partitionKey-%d", j/5 ));
////            putRecordRequest.setSequenceNumberForOrdering( sequenceNumberOfPreviousRecord );
////            PutRecordResult putRecordResult = client.putRecord( putRecordRequest );
////            sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber();
//        }
//    }

    public static void main(String[] args) {
        SampleProducer sampleProducer = new SampleProducer();
        sampleProducer.publishRecord();
    }

    private void publishRecord() {
        Region region = Region.of("us-west-2");
        this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));
        String streamName = "naga-test";
        PutRecordRequest request = PutRecordRequest.builder()
                .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                .streamName(streamName)
                .data(SdkBytes.fromByteArray("this is test data".getBytes()))
                .build();
        try {
            kinesisClient.putRecord(request).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
