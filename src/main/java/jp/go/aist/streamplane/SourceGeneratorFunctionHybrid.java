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

    private final OutputStream defaultOutputStream;
    private final AtomicInteger indexIterator;
    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> outputQueues;

    private final Map<String, String> operatorMeta; // status: running/paused,
    private final Map<String, String> defaultOutputChannelMeta;
    private String currentOutputStreamId;
    private Map<String, String> currentOutputChannelMeta;

    private Ignite ignite;

    private boolean jobPaused;

    public SourceGeneratorFunctionHybrid(boolean jobPaused, OutputStream defaultOutputStream) {
        this.jobPaused = jobPaused;
        this.defaultOutputStream = defaultOutputStream;
        this.indexIterator = new AtomicInteger(0);
        this.outputQueues = new ConcurrentHashMap<>();
        this.operatorMeta = new ConcurrentHashMap<>();
        this.defaultOutputChannelMeta = new ConcurrentHashMap<>();
        this.currentOutputChannelMeta = new ConcurrentHashMap<>();
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

        currentOutputStreamId = operatorMetaCache.getAndPutIfAbsent("output-stream-" + subtaskIndex, defaultOutputStream.getId());
        if(currentOutputStreamId == null){
            currentOutputStreamId = defaultOutputStream.getId();
        }
        operatorMeta.put("output-stream-" + subtaskIndex, currentOutputStreamId);
        if(currentOutputStreamId.equals(defaultOutputStream.getId())){
            this.currentOutputChannelMeta = defaultOutputChannelMeta;
        }

        createContinousQuery(operatorMetaCacheKey, operatorMeta); //cache_id: <job_id>-task-<this_task_name>

        //only listen changes to the default output stream
        createContinousQuery(defaultOutputStream.getId(), defaultOutputChannelMeta);
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
                                if(e.getKey().equals("output-stream-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())){
                                    currentOutputStreamId = e.getValue();
                                    if(currentOutputStreamId.equals(defaultOutputStream.getId())){
                                        currentOutputChannelMeta = defaultOutputChannelMeta;
                                    } else {
                                        IgniteCache<String, String> outputCache = ignite.getOrCreateCache(currentOutputStreamId);
                                        currentOutputChannelMeta = new ConcurrentHashMap<>();
                                        outputCache.forEach(entry -> {currentOutputChannelMeta.put(entry.getKey(), entry.getValue());});
                                    }
                                    //update the outputQueues
                                    for(String index : currentOutputChannelMeta.keySet()){
                                        String queueKey = currentOutputStreamId + "-" + index;
                                        outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                                    }
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
    public void run(SourceContext<Tuple3<Integer, Integer, String>> sourceContext) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        while (jobPaused) {
            if(subtaskIndex == 0) {
                for (int i = 0; i < defaultOutputStream.getParallelism(); i++) {
                    sourceContext.collect(Tuple3.of(i, -1, null));
                }
            }
            synchronized (operatorMeta) {
                operatorMeta.wait();
            }
        }
        int nextIndex = Math.abs(indexIterator.getAndIncrement());
        String nextSentence = WordCountData.WORDS[nextIndex % WordCountData.WORDS.length];

        while (true){
            Integer destChannel = defaultOutputStream.getNextChannelToSendTo(nextIndex);
            String queueKey = currentOutputStreamId + "-" + destChannel; //<stream_id>-<dest_channel>
            if (currentOutputChannelMeta.containsKey(destChannel.toString())) {
                if(outputQueues.containsKey(queueKey)) {
                    outputQueues.get(queueKey).add(Tuple3.of(destChannel, 1, nextSentence));
                } else { //switching: raw >> imdg, or new output channel switching
                    outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                    sourceContext.collect(Tuple3.of(destChannel, 1, nextSentence));
                }
            } else {
                if(outputQueues.containsKey(queueKey)) { // switching: imdg >> raw
                    outputQueues.remove(queueKey).add(Tuple3.of(destChannel, 0, nextSentence));
                } else {
                    sourceContext.collect(Tuple3.of(destChannel, 0, nextSentence));
                }
            }

            if(operatorMeta.getOrDefault("instance-status-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "Running").equals("Paused")){
                synchronized (operatorMeta) {
                    operatorMeta.wait();
                }
            }

//            Thread.sleep(1000);
            nextIndex = Math.abs(indexIterator.getAndIncrement());
            nextSentence = WordCountData.WORDS[nextIndex % WordCountData.WORDS.length];
        }
    }

    @Override
    public void cancel() {

    }
}
