package jp.go.aist.streamplane;

import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import java.util.UUID;

public class Test {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("taskmanager.numberOfTaskSlots", 1);
        conf.setInteger("local.number-taskmanager", 2); // for testing more than 1 task manager
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final int auctionSrcRate = 10;


        DataStream<Auction> auctionSource = env.addSource(new AuctionSourceFunction(auctionSrcRate))
                .name("AuctionSource")
                .uid("AuctionSource");
//                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new AuctionTimestampExtractor(Time.seconds(0))));

        auctionSource.addSink(new DiscardingSink<>());

        env.execute("test job");
    }

//    public static void main(String[] args) {
//        System.out.println(System.currentTimeMillis());
//        Integer p = Integer.valueOf(args[0]);
//        String jobId1 = args[1];
//        String tokenizerOutputId = args[2];
//        String counterOutputId = args[3];
//        String jobId2 = args[4];
//
//        Ignite ignite = Ignition.getOrStart(ImdgConfig.CONFIG());
//
//        //job 1
//        IgniteCache<String, String> tokenizerOutputMetaCache = ignite.getOrCreateCache(tokenizerOutputId);
//        for (Integer i = 0; i < p; i++) {
//            tokenizerOutputMetaCache.putIfAbsent(i.toString(), tokenizerOutputId + "-" + i);
//        }
//        IgniteCache<String, String> counterOutputMetaCache = ignite.getOrCreateCache(counterOutputId);
//        for (Integer i = 0; i < p; i++) {
//            counterOutputMetaCache.putIfAbsent(i.toString(), counterOutputId + "-" + i);
//        }
//        IgniteCache<String, String> counterOperatorCache = ignite.getOrCreateCache(jobId1 + "-task-Counter");
//        for (Integer i = 0; i < p; i++) {
//            counterOperatorCache.put("instance-status-" + i, "Paused"); //<instance_index>,<status>
//        }
//
//        //job 2
//        IgniteCache<String, String> counterOperatorCache2 = ignite.getOrCreateCache(jobId2 + "-task-Counter");
//        for (Integer i = 0; i < p; i++) {
//            counterOperatorCache2.put("input-stream-" + i, tokenizerOutputId);
//            counterOperatorCache2.put("output-stream-" + i, counterOutputId);
//            counterOperatorCache2.put("instance-status-" + i, "Running");
//        }
//
//    }

}
