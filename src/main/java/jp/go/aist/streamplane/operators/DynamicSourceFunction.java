package jp.go.aist.streamplane.operators;

import jp.go.aist.streamplane.ImdgConfig;
import jp.go.aist.streamplane.stream.OutputStream;
import jp.go.aist.streamplane.events.ActivatorEvent;
import jp.go.aist.streamplane.events.DataTuple;
import jp.go.aist.streamplane.events.StreamEvent;
import jp.go.aist.streamplane.stream.partitioners.StreamPlaneForwardPartitioner;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public abstract class DynamicSourceFunction<OUT extends Tuple> extends RichSourceFunction<StreamEvent> {

    private final OutputStream defaultOutputStream;
    private final AtomicLong indexIterator;
    private final Map<String, IgniteQueue<StreamEvent>> outputQueues;

    private final Map<String, String> operatorMeta; // status: running/paused,
    private final Map<String, String> defaultOutputChannelMeta;
    private String currentOutputStreamId;
    private Map<String, String> currentOutputChannelMeta;

    private Ignite ignite;

    private boolean jobPaused;

    private OperatorInstanceInfo operatorInstanceInfo;

    private Collector<StreamEvent> collector;

    private IgniteCache<String, Object> states;

    protected DynamicSourceFunction(OutputStream defaultOutputStream) {
        this(defaultOutputStream, false);
    }

    protected DynamicSourceFunction(OutputStream defaultOutputStream, boolean jobPaused) {
        this.defaultOutputStream = defaultOutputStream;
        this.jobPaused = jobPaused;
        this.indexIterator = new AtomicLong(0);
        this.outputQueues = new ConcurrentHashMap<>();
        this.operatorMeta = new ConcurrentHashMap<>();
        this.defaultOutputChannelMeta = new ConcurrentHashMap<>();
        this.currentOutputChannelMeta = new ConcurrentHashMap<>();
    }

    public Object putState(String stateKey, Object stateValue) {
        states.put(stateKey, stateValue);
        return stateValue;
    }

    public Object getState(String stateKey) {
        return states.get(stateKey);
    }

    public String getStateIdPrefix(){
        return getRuntimeContext().getJobInfo().getJobId().toString() + "-" +
                getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.operatorInstanceInfo = new OperatorInstanceInfo(
                getRuntimeContext().getTaskInfo().getTaskName(),
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks());
        ignite = Ignition.getOrStart(ImdgConfig.CONFIG());
        states = ignite.getOrCreateCache(getStateIdPrefix() + "-state");
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
                                    String newOutputStreamId = e.getValue();
                                    Map<String, String> newOutputChannelMeta;
                                    if(newOutputStreamId.equals(defaultOutputStream.getId())){
                                        newOutputChannelMeta = defaultOutputChannelMeta;
                                    } else {
                                        IgniteCache<String, String> outputCache = ignite.getOrCreateCache(newOutputStreamId);
                                        newOutputChannelMeta = new ConcurrentHashMap<>();
                                        outputCache.forEach(entry -> {newOutputChannelMeta.put(entry.getKey(), entry.getValue());});
                                    }
                                    //update the outputQueues
                                    for(String index : newOutputChannelMeta.keySet()){
                                        String queueKey = newOutputStreamId + "-" + index; //the physical output id
                                        outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                                    }

                                    currentOutputStreamId = newOutputStreamId;
                                    currentOutputChannelMeta = newOutputChannelMeta;
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

    public abstract OUT generateNextTuple(Long index);

    @Override
    public void run(SourceContext<StreamEvent> sourceContext) throws Exception {
        if (jobPaused) {
            if(getRuntimeContext().getIndexOfThisSubtask() == 0) {
                for (int i = 0; i < defaultOutputStream.getParallelism(); i++) {
                    sourceContext.collect(new ActivatorEvent(operatorInstanceInfo, i, 0));
                }
            }
            synchronized (operatorMeta) {
                operatorMeta.wait();
            }
        }
        long nextIndex = Math.abs(indexIterator.getAndIncrement());
        Tuple nextTuple = generateNextTuple(nextIndex);

        while (nextTuple != null) {
            Integer destChannel;
            if(defaultOutputStream.getPartitioner() instanceof StreamPlaneForwardPartitioner){
                destChannel = defaultOutputStream.getNextChannelToForwardTo(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            } else {
                destChannel = defaultOutputStream.getNextChannelToSendTo(nextTuple);
            }
            String queueKey = currentOutputStreamId + "-" + destChannel; //<stream_id>-<dest_channel>
            if (currentOutputChannelMeta.containsKey(destChannel.toString())) {
                if(outputQueues.containsKey(queueKey)) {
                    outputQueues.get(queueKey).add(new DataTuple<Tuple>(operatorInstanceInfo, destChannel, 1, nextTuple));
                } else { //switching: raw >> imdg, or new output channel switching
                    outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                    sourceContext.collect(new DataTuple<Tuple>(operatorInstanceInfo, destChannel, 1, nextTuple));
                }
            } else {
                if(outputQueues.containsKey(queueKey)) { // switching: imdg >> raw
                    outputQueues.remove(queueKey).add(new DataTuple<Tuple>(operatorInstanceInfo, destChannel, 0, nextTuple));
                } else {
                    sourceContext.collect(new DataTuple<Tuple>(operatorInstanceInfo, destChannel, 0, nextTuple));
                }
            }

            if(operatorMeta.getOrDefault("instance-status-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "Running").equals("Paused")){
                synchronized (operatorMeta) {
                    operatorMeta.wait();
                }
            }

            nextIndex = Math.abs(indexIterator.getAndIncrement());
            nextTuple = generateNextTuple(nextIndex);
        }
    }

    @Override
    public void cancel() {

    }

    public Ignite getIgniteClient() {
        return ignite;
    }
}
