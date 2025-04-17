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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SinkFunctionHybrid extends RichSinkFunction<Tuple3<Integer, Integer, String>> {

    private String defaultInputStreamId;
    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> inputQueues;

    private final Map<String, String> operatorMeta;

    private Ignite ignite;

    public SinkFunctionHybrid(String defaultInputStreamId){
        this.defaultInputStreamId = defaultInputStreamId;
        this.inputQueues = new ConcurrentHashMap<>();
        this.operatorMeta = new ConcurrentHashMap<>();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ignite = Ignition.getOrStart(ImdgConfig.CONFIG());
        String operatorMetaCacheKey = getRuntimeContext().getJobInfo().getJobId().toString() + "-task-" + this.getRuntimeContext().getTaskInfo().getTaskName();
        IgniteCache<String, String> operatorMetaCache = ignite.getOrCreateCache(operatorMetaCacheKey);

        String instanceStatus = operatorMetaCache.getAndPutIfAbsent("instance-status-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "Running");
        if(instanceStatus != null && instanceStatus.equals("Paused")){
            operatorMeta.put("instance-status-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "Paused");
        } else {
            operatorMeta.put("instance-status-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "Running");
        }

        String inputStream = operatorMetaCache.getAndPutIfAbsent("input-stream", this.defaultInputStreamId);
        if(inputStream == null){
            inputStream = this.defaultInputStreamId;
        }
        operatorMeta.put("input-stream", inputStream);

        createContinousQuery(operatorMetaCacheKey, operatorMeta); //cache_id: <job_id>-task-<this_task_name>
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
                                if(e.getKey().equals("instance-status-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()) && e.getValue().equals("Running")){
                                    synchronized (operatorMeta) {
                                        operatorMeta.notifyAll();
                                    }
                                }
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

            System.out.println(channelType + " | " + word);

            if(operatorMeta.getOrDefault("instance-status-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "Running").equals("Paused")){
                synchronized (operatorMeta) {
                    operatorMeta.wait();
                }
            }

            //get next tuple
            String inputStreamKey = operatorMeta.getOrDefault("input-stream", this.defaultInputStreamId);
            String inputQueueKey = inputStreamKey + "-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            if (channelType == 1) { // imdg
                IgniteQueue<Tuple3<Integer, Integer, String>> queue = inputQueues.computeIfAbsent(inputQueueKey, k -> ignite.queue(inputQueueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                Tuple3<Integer, Integer, String> tuple = queue.take();
                channelIndex = tuple.f0;
                channelType = tuple.f1;
                word = tuple.f2;
                continue;
            } else {
                if(inputQueues.containsKey(inputQueueKey)) {
                    IgniteQueue<Tuple3<Integer, Integer, String>> queue = inputQueues.get(inputQueueKey);
                    if (!queue.isEmpty()) { //process inputs in IMDG if exists
                        Tuple3<Integer, Integer, String> tuple = queue.take();
                        channelIndex = tuple.f0;
                        channelType = tuple.f1;
                        word = tuple.f2;
                        continue;
                    } else {
                        inputQueues.remove(inputQueueKey).close();
                    }
                }
            }
        } while (channelType == 1);
    }

}
