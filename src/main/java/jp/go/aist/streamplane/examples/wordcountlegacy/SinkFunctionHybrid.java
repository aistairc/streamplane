package jp.go.aist.streamplane.examples.wordcountlegacy;

import jp.go.aist.streamplane.imdg.ImdgConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class SinkFunctionHybrid extends RichSinkFunction<Tuple3<Integer, Integer, String>> {

    private String defaultInputStreamId;
    private IgniteQueue<Tuple3<Integer, Integer, String>> inputQueue;

    private final Map<String, String> operatorMeta;

    private Ignite ignite;

    public SinkFunctionHybrid(String defaultInputStreamId){
        this.defaultInputStreamId = defaultInputStreamId;
        this.operatorMeta = new ConcurrentHashMap<>();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ignite = Ignition.getOrStart(ImdgConfig.CONFIG());
        String operatorMetaCacheKey = getRuntimeContext().getJobInfo().getJobId().toString() + "-task-" + this.getRuntimeContext().getTaskInfo().getTaskName();
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        IgniteCache<String, String> operatorMetaCache = ignite.getOrCreateCache(operatorMetaCacheKey);

        operatorMetaCache.put("parallelism", String.valueOf(getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks()));

        operatorMetaCache.put("task-name-" + subtaskIndex, getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks());
        operatorMetaCache.put("allocation-id-" + subtaskIndex, getRuntimeContext().getTaskInfo().getAllocationIDAsString());

        String instanceStatus = operatorMetaCache.getAndPutIfAbsent("instance-status-" + subtaskIndex, "Running");
        if(instanceStatus != null && instanceStatus.equals("Paused")){
            operatorMeta.put("instance-status-" + subtaskIndex, "Paused");
        } else {
            operatorMeta.put("instance-status-" + subtaskIndex, "Running");
        }

        String inputStream = operatorMetaCache.getAndPutIfAbsent("input-stream-" + subtaskIndex, this.defaultInputStreamId);
        if(inputStream == null){
            inputStream = this.defaultInputStreamId;
        }
        operatorMeta.put("input-stream-" + subtaskIndex, inputStream);
        inputQueue = ignite.queue(inputStream + "-" + subtaskIndex, 0, ImdgConfig.QUEUE_CONFIG());

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
                                entries.put(e.getKey(), e.getValue());
                                if(e.getKey().equals("instance-status-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()) && e.getValue().equals("Running")){
                                    synchronized (operatorMeta) {
                                        operatorMeta.notifyAll();
                                    }
                                }

                                if(e.getKey().equals("input-stream-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())){
                                    inputQueue = ignite.queue(e.getValue() + "-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), 0, ImdgConfig.QUEUE_CONFIG());
                                }
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
            if(channelType == -1) { //paused job
                synchronized (operatorMeta) {
                    operatorMeta.wait();
                    channelType = 1; //switch to IMDG when resumed
                }
            } else {
                if (channelIndex != this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()) {
                    throw new Exception("Channel index of received tuples does not match this subtask");
                }

//                System.out.printf("[%s] Input: %s, Message: %s\n", getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks(),
//                        channelType == 1 ? "In-memory": "Built-in",
//                        word);

            }

            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            if(operatorMeta.getOrDefault("instance-status-" + subtaskIndex, "Running").equals("Paused")){
                synchronized (operatorMeta) {
                    operatorMeta.wait();
                }
            }

            //get next tuple
            if (channelType == 1) { // imdg
                Tuple3<Integer, Integer, String> tuple = inputQueue.take();
                channelIndex = tuple.f0;
                channelType = tuple.f1;
                word = tuple.f2;
                continue;
            } else {
                if (!inputQueue.isEmpty()) { //process inputs in IMDG if exists
                    Tuple3<Integer, Integer, String> tuple = inputQueue.take();
                    channelIndex = tuple.f0;
                    channelType = tuple.f1;
                    word = tuple.f2;
                    continue;
                } else {
//                    inputQueue.close();
                }
            }
        } while (channelType == 1);
    }

}
