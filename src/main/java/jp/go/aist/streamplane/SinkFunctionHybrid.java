package jp.go.aist.streamplane;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
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

public class SinkFunctionHybrid extends RichSinkFunction<Tuple3<Integer, Integer, String>> {

    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> inputQueues;

    private final Map<String, String> operatorMeta;
    private final Map<String, Map<String, String>> channelMetas;

    private Ignite ignite;

    private String cachePrefix;

    private CollectionConfiguration queueCfg;

//    private volatile long prevTimestamp = 0;

    public SinkFunctionHybrid(){
        this.inputQueues = new ConcurrentHashMap<>();
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
    public void invoke(Tuple3<Integer, Integer, String> input, Context context) throws Exception {
        Integer channelIndex = input.f0;
        Integer channelType = input.f1;
        String word = input.f2;
        do {
            if(channelIndex != this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()){
                throw new Exception("Channel index of received tuples does not match this subtask");
            }

//            System.out.println(channelType + " | tp: " + (System.nanoTime()-prevTimestamp));
//            prevTimestamp = System.nanoTime();

            System.out.println(channelType + " | " + word);

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
