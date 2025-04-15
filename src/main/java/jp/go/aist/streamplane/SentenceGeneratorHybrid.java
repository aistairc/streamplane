package jp.go.aist.streamplane;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import rex.takdir.flinkimdg.WordCountData;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SentenceGeneratorHybrid implements GeneratorFunction<Long, Tuple3<Integer, Integer, String>> {

    private final DestinationTask[] destinationTasks;
    private final Map<Integer, ClientCache<String, Tuple3<Integer, Integer, String>>> outputCaches;

    private int nextChannelToSendTo;

    private final Set<Integer> cacheIndexes;

    private IgniteClient igniteClient;

    private final AtomicInteger indexIterator;

    public SentenceGeneratorHybrid(DestinationTask... destinationTasks) {
        this.destinationTasks = destinationTasks;
        this.outputCaches = new ConcurrentHashMap<>();
        this.cacheIndexes = new HashSet<>();
        this.indexIterator = new AtomicInteger(0);
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        String ip = InetAddress.getLocalHost().getHostAddress();
        ClientConfiguration cfg = new ClientConfiguration().setAddresses(ip+":10800");
        igniteClient = Ignition.startClient(cfg);
//        this.nextChannelToSendTo = ThreadLocalRandom.current().nextInt(destParallelism);
    }

    @Override
    public Tuple3<Integer, Integer, String> map(Long index) throws Exception {
        String nextSentence = WordCountData.WORDS[Math.abs(indexIterator.getAndIncrement()) % WordCountData.WORDS.length];
        for(DestinationTask destinationTask : destinationTasks){
            int destChannel = destinationTask.getNextChannelToSendTo(index.intValue());
            int channelType = 0; // 0 = raw, 1 = imdg

        }




//        this.nextChannelToSendTo = (this.nextChannelToSendTo + 1) % this.destParallelism;
        while (cacheIndexes.contains(this.nextChannelToSendTo)){
            if(outputCaches.containsKey(this.nextChannelToSendTo)){
                outputCaches.get(nextChannelToSendTo).put(UUID.randomUUID().toString(), Tuple3.of(nextChannelToSendTo, 1, WordCountData.WORDS[Math.abs(indexIterator.getAndIncrement()) % WordCountData.WORDS.length]));
            } else { //switching: raw >> imdg
//                outputCaches.put(this.nextChannelToSendTo, igniteClient.getOrCreateCache(destTaskName + nextChannelToSendTo));
                return Tuple3.of(nextChannelToSendTo, 1, WordCountData.WORDS[Math.abs(indexIterator.getAndIncrement()) % WordCountData.WORDS.length]);
            }
//            this.nextChannelToSendTo = (this.nextChannelToSendTo + 1) % this.destParallelism;
        }
        if(outputCaches.containsKey(this.nextChannelToSendTo)){ // switching: imdg >> raw
            outputCaches.remove(nextChannelToSendTo).put(UUID.randomUUID().toString(), Tuple3.of(nextChannelToSendTo, 0, WordCountData.WORDS[Math.abs(indexIterator.getAndIncrement()) % WordCountData.WORDS.length]));
//            this.nextChannelToSendTo = (this.nextChannelToSendTo + 1) % this.destParallelism;
        }
        return Tuple3.of(nextChannelToSendTo, 0, WordCountData.WORDS[Math.abs(indexIterator.getAndIncrement()) % WordCountData.WORDS.length]);
    }
}
