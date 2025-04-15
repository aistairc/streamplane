package jp.go.aist.streamplane;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CollectionConfiguration;
import rex.takdir.flinkimdg.WordCountData;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class SourceGeneratorFunctionHybrid extends RichSourceFunction<Tuple3<Integer, Integer, String>> {

    private final DestinationTask[] destinationTasks;
    private final AtomicInteger indexIterator;
    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> outputQueues;

    private final Map<String, String> operatorMeta; // status: run/idle,
    private final Map<String, Map<String, String>> channelMetas;

    private Ignite ignite;

    private String cachePrefix;

    private CollectionConfiguration queueCfg;

    public SourceGeneratorFunctionHybrid(DestinationTask... destinationTasks) {
        this.destinationTasks = destinationTasks;
        this.indexIterator = new AtomicInteger(0);
        this.outputQueues = new ConcurrentHashMap<>();
        this.operatorMeta = new ConcurrentHashMap<>();
        this.channelMetas = new ConcurrentHashMap<>();
        this.queueCfg = new CollectionConfiguration();
        this.queueCfg.setCollocated(true);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        cachePrefix = getRuntimeContext().getJobInfo().getJobId().toString();
        ignite = Ignition.getOrStart(ImdgConfig.CONFIG());
        createContinousQuery(cachePrefix + "-" + this.getRuntimeContext().getTaskInfo().getTaskName(), operatorMeta); //cache_id: <job_id>-<this_task>
        for(DestinationTask destinationTask : destinationTasks) {
            String key = cachePrefix + "-" + destinationTask.getTaskName() + "-OUT"; //cache_id: <job_id>-<dest_task>-OUT
            createContinousQuery(key, channelMetas.computeIfAbsent(key, k -> new ConcurrentHashMap<>()));
        }
    }

    private void createContinousQuery(String cacheKey, final Map<String, String> entries){
        IgniteCache<String, String> cache = ignite.getOrCreateCache(cacheKey);
        ContinuousQuery<String, String> query = new ContinuousQuery<>();
        query.setInitialQuery(new ScanQuery<>())
                .setLocalListener(new CacheEntryUpdatedListener<String, String>() {
                    @Override
                    public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends String>> evts) throws CacheEntryListenerException {
                        for(CacheEntryEvent<? extends String, ? extends String> e : evts){
                            if(e.getEventType() == EventType.REMOVED) {
                                entries.remove(e.getKey());
                            } else {
                                entries.put(e.getKey(), e.getValue());
                            }
                        }
                    }
                });
        cache.query(query).forEach(new Consumer<Cache.Entry<String, String>>() {
            @Override
            public void accept(Cache.Entry<String, String> e) {
                entries.put(e.getKey(), e.getValue());
            }
        });
    }

    @Override
    public void run(SourceContext<Tuple3<Integer, Integer, String>> sourceContext) throws Exception {
        int nextIndex = Math.abs(indexIterator.getAndIncrement());
        String nextSentence = WordCountData.WORDS[nextIndex % WordCountData.WORDS.length];
        while (true){
//            Thread.sleep(1000);
            for(DestinationTask destinationTask : destinationTasks){
                Integer destChannel = destinationTask.getNextChannelToSendTo(nextIndex);
                String prefix = cachePrefix + "-" + destinationTask.getTaskName() + "-OUT";
                String queueKey = prefix + "-" + destChannel; //<job_id>-<dest_task>-OUT-<dest_channel>
                if(channelMetas.containsKey(prefix)){
                    Map<String, String> channelMeta = channelMetas.get(prefix);
                    if (channelMeta.containsKey(destChannel.toString())) {
                        if(outputQueues.containsKey(queueKey)) {
                            IgniteQueue<Tuple3<Integer, Integer, String>> queue = outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, queueCfg));
                            queue.add(Tuple3.of(destChannel, 1, nextSentence));
                        } else { //switching: raw >> imdg
                            outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, queueCfg));
                            sourceContext.collect(Tuple3.of(destChannel, 1, nextSentence));
                        }
                    } else {
                        if(outputQueues.containsKey(queueKey)) { // switching: imdg >> raw
                            outputQueues.remove(queueKey).add(Tuple3.of(destChannel, 0, nextSentence));
                        } else {
                            sourceContext.collect(Tuple3.of(destChannel, 0, nextSentence));
                        }
                    }
                } else {
                    if(outputQueues.containsKey(queueKey)) { // switching: imdg >> raw
                        outputQueues.remove(queueKey).add(Tuple3.of(destChannel, 0, nextSentence));
                    } else {
                        sourceContext.collect(Tuple3.of(destChannel, 0, nextSentence));
                    }
                }
            }
            nextIndex = Math.abs(indexIterator.getAndIncrement());
            nextSentence = WordCountData.WORDS[nextIndex % WordCountData.WORDS.length];
        }
    }

    @Override
    public void cancel() {

    }
}
