package jp.go.aist.streamplane;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.ignite.*;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CollectionConfiguration;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class CounterProcessFunctionHybrid extends ProcessFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>> {

    private final DestinationTask[] destinationTasks;
    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> outputQueues;
    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> inputQueues;

    private final Map<String, String> operatorMeta;
    private final Map<String, Map<String, String>> channelMetas;

    private Ignite ignite;

    private String cachePrefix;

    private CollectionConfiguration queueCfg;

    private final Map<String, IgniteAtomicLong> counters;

    public CounterProcessFunctionHybrid(DestinationTask... destinationTasks){
        this.destinationTasks = destinationTasks;
        this.inputQueues = new ConcurrentHashMap<>();
        this.outputQueues = new ConcurrentHashMap<>();
        this.operatorMeta = new ConcurrentHashMap<>();
        this.channelMetas = new ConcurrentHashMap<>();
        this.queueCfg = new CollectionConfiguration();
        this.queueCfg.setCollocated(true);
        this.counters = new ConcurrentHashMap<>();
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
    public void processElement(Tuple3<Integer, Integer, String> input, ProcessFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>>.Context context, Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {
        Integer channelIndex = input.f0;
        Integer channelType = input.f1;
        String word = input.f2;
        do {
            if(channelIndex != this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()){
                throw new Exception("Channel index of received tuples does not match this subtask");
            }
            final String atomicKey = word;
            Long count = counters.computeIfAbsent(word, k -> ignite.atomicLong(atomicKey, 0, true)).incrementAndGet();
            String msg = word + ":" + count;
            for(DestinationTask destinationTask : destinationTasks){
                Integer destChannel = destinationTask.getNextChannelToSendTo(word);
                String prefix = cachePrefix + "-" + destinationTask.getTaskName() + "-OUT";
                String queueKey = prefix + "-" + destChannel; //<job_id>-<dest_task>-OUT-<dest_channel>
                if(channelMetas.containsKey(prefix)){
                    Map<String, String> channelMeta = channelMetas.get(prefix);
                    if (channelMeta.containsKey(destChannel.toString())) {
                        if(outputQueues.containsKey(queueKey)) {
                            IgniteQueue<Tuple3<Integer, Integer, String>> queue = outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, queueCfg));
                            queue.add(Tuple3.of(destChannel, 1, msg));
                        } else { //switching: raw >> imdg
                            outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, queueCfg));
                            collector.collect(Tuple3.of(destChannel, 1, msg));
                        }
                    } else {
                        if(outputQueues.containsKey(queueKey)) { // switching: imdg >> raw
                            outputQueues.remove(queueKey).add(Tuple3.of(destChannel, 0, msg));
                        } else {
                            collector.collect(Tuple3.of(destChannel, 0, msg));
                        }
                    }
                } else {
                    if(outputQueues.containsKey(queueKey)) { // switching: imdg >> raw
                        outputQueues.remove(queueKey).add(Tuple3.of(destChannel, 0, msg));
                    } else {
                        collector.collect(Tuple3.of(destChannel, 0, msg));
                    }
                }
            }

            String inputQueueKey = cachePrefix + "-" + this.getRuntimeContext().getTaskInfo().getTaskName() + "-OUT-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            if (channelType == 1) { // imdg
                IgniteQueue<Tuple3<Integer, Integer, String>> queue = inputQueues.computeIfAbsent(inputQueueKey, k -> ignite.queue(inputQueueKey, 0, queueCfg));
                Tuple3<Integer, Integer, String> tuple = queue.take();
                channelIndex = tuple.f0;
                channelType = tuple.f1;
                word = tuple.f2;
            } else {
                if(inputQueues.containsKey(inputQueueKey)) {
                    IgniteQueue<Tuple3<Integer, Integer, String>> queue = inputQueues.get(inputQueueKey);
                    if (!queue.isEmpty()) {
                        Tuple3<Integer, Integer, String> tuple = queue.take();
                        channelIndex = tuple.f0;
                        channelType = tuple.f1;
                        word = tuple.f2;
                    } else {
                        inputQueues.remove(inputQueueKey).close();
                    }
                }
            }
        } while (channelType == 1);
    }
}
