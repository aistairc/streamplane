package jp.go.aist.streamplane.operators;

import jp.go.aist.streamplane.imdg.ImdgConfig;
import jp.go.aist.streamplane.stream.InputStream;
import jp.go.aist.streamplane.stream.OutputStream;
import jp.go.aist.streamplane.events.ActivatorEvent;
import jp.go.aist.streamplane.events.DataTuple;
import jp.go.aist.streamplane.events.QueueStopperEvent;
import jp.go.aist.streamplane.events.StreamEvent;
import jp.go.aist.streamplane.stream.partitioners.StreamPlaneForwardPartitioner;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class DynamicProcessFunction<IN extends Tuple, OUT extends Tuple> extends ProcessFunction<StreamEvent, StreamEvent> {

    private InputStream[] defaultInputStreams;
    private final OutputStream defaultOutputStream;
    private final Map<String, IgniteQueue<StreamEvent>> outputQueues;
    private final Map<String, IgniteQueue<StreamEvent>> inputQueues;

    private final Map<String, String> operatorMeta; // status: running/paused,
    private final Map<String, String> defaultOutputChannelMeta;
    private String currentOutputStreamId;
    private Map<String, String> currentOutputChannelMeta;

    private Ignite ignite;

    private boolean paused = false;

    private OperatorInstanceInfo operatorInstanceInfo;

    private Collector<StreamEvent> collector;

    private IgniteCache<String, Object> states;

    private String jobId = null; //for testing purpose

    private boolean debug = false;

    protected DynamicProcessFunction(InputStream[] defaultInputStreams, OutputStream defaultOutputStream){
        this(defaultInputStreams, defaultOutputStream, false, null);
    }

    protected DynamicProcessFunction(InputStream[] defaultInputStreams, OutputStream defaultOutputStream, boolean paused){
        this(defaultInputStreams, defaultOutputStream, paused, null);
    }

    protected DynamicProcessFunction(InputStream[] defaultInputStreams, OutputStream defaultOutputStream, boolean paused, String customJobId){
        this.jobId = customJobId;
        this.defaultInputStreams = defaultInputStreams;
        this.defaultOutputStream = defaultOutputStream;
        this.paused = paused;
        this.outputQueues = new ConcurrentHashMap<>();
        this.inputQueues = new ConcurrentHashMap<>();
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
        return getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.operatorInstanceInfo = new OperatorInstanceInfo(
                getRuntimeContext().getTaskInfo().getTaskName(),
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks());
        ignite = Ignition.getOrStart(ImdgConfig.CONFIG());
        states = ignite.getOrCreateCache(getStateIdPrefix() + "-state");
        if(jobId == null){
            jobId = getRuntimeContext().getJobInfo().getJobId().toString();
        }
        String operatorMetaCacheKey = jobId + "-task-" + this.getRuntimeContext().getTaskInfo().getTaskName();
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        IgniteCache<String, String> operatorMetaCache = ignite.getOrCreateCache(operatorMetaCacheKey);

        operatorMetaCache.put("parallelism", String.valueOf(getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks()));

        operatorMetaCache.put("task-name-" + subtaskIndex, getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks());
        operatorMetaCache.put("allocation-id-" + subtaskIndex, getRuntimeContext().getTaskInfo().getAllocationIDAsString());

        String instanceStatus = operatorMetaCache.getAndPutIfAbsent("instance-status-" + subtaskIndex, paused ? "Paused" : "Running");
        if(instanceStatus != null && instanceStatus.equals("Paused")){
            operatorMeta.put("instance-status-" + subtaskIndex, "Paused");
        } else {
            operatorMeta.put("instance-status-" + subtaskIndex, "Running");
        }

        String inputStreamIds = operatorMetaCache.getAndPutIfAbsent("input-stream-" + subtaskIndex, Arrays.stream(defaultInputStreams).map(InputStream::getId).collect(Collectors.joining(",")));
        if(inputStreamIds == null){
            inputStreamIds = Arrays.stream(defaultInputStreams).map(InputStream::getId).collect(Collectors.joining(","));
        }
        operatorMeta.put("input-stream-" + subtaskIndex, inputStreamIds);
        for(String inputStreamId : inputStreamIds.split(",")){
            IgniteQueue<StreamEvent> queue = ignite.queue(inputStreamId + "-" + subtaskIndex, 0, ImdgConfig.QUEUE_CONFIG());
            inputQueues.put(inputStreamId, queue);
            new QueueListenerThread(queue).start();
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
//                                System.out.println("New entry: " + e.getKey() + ": " + e.getValue());
                                int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
                                entries.put(e.getKey(), e.getValue());
                                if(e.getKey().equals("instance-status-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())){
                                    if(e.getValue().equals("Running")){
                                        paused = false;
                                        synchronized (operatorMeta) {
                                            operatorMeta.notifyAll();
                                        }
                                    }
                                    if(e.getValue().equals("Paused")){
                                        paused = true;
                                    }
                                }

                                if(e.getKey().equals("instance-output-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())){
                                    debug = e.getValue().equals("with-log");
                                }

                                if(e.getKey().equals("input-stream-" + subtaskIndex)){
                                    String[] newInputStreamIds = e.getValue().split(","); //the logical input IDs == the logical output stream IDs of its parents
                                    for(String inputStreamId : newInputStreamIds){
                                        String queueKey = inputStreamId + "-" + subtaskIndex;
                                        if(!inputQueues.containsKey(queueKey)){
                                            IgniteQueue<StreamEvent> newQueue = ignite.queue(queueKey, 0, ImdgConfig.QUEUE_CONFIG());
                                            inputQueues.put(queueKey, newQueue);
                                            new QueueListenerThread(newQueue).start();
                                        }
                                    }
                                    //stop removed queue threads
                                    Set<String> newIdSet = Arrays.stream(newInputStreamIds).collect(Collectors.toSet());
                                    for(String queueId : inputQueues.keySet()){
                                        if(!newIdSet.contains(queueId)){
                                            inputQueues.remove(queueId).add(new QueueStopperEvent(operatorInstanceInfo));
                                        }
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

    public abstract List<OUT> processDataTuple(IN input);

    public synchronized void processInputEvent(StreamEvent event) throws Exception {
        if(paused) { //paused jobx
            synchronized (operatorMeta) {
                operatorMeta.wait();
                return;
            }
        }

        Integer channelIndex = event.getDestinationInstanceIndex();
        if(channelIndex != this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()){
            throw new Exception("Channel index of received tuples does not match this subtask");
        }

        if(event instanceof DataTuple){
            DataTuple dataTuple = (DataTuple) event;
            List<OUT> newTuples = processDataTuple((IN) dataTuple.getData());
            if(newTuples != null){
                for(Tuple nextTuple : newTuples){
                    Integer destChannel;
                    if(defaultOutputStream.getPartitioner() instanceof StreamPlaneForwardPartitioner){
                        destChannel = defaultOutputStream.getNextChannelToForwardTo(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
                    } else {
                        destChannel = defaultOutputStream.getNextChannelToSendTo(nextTuple);
                    }
                    String queueKey = currentOutputStreamId + "-" + destChannel; //<stream_id>-<dest_channel>
                    if (currentOutputChannelMeta.containsKey(destChannel.toString())) {
                        if(outputQueues.containsKey(queueKey)) { //imdg
                            outputQueues.get(queueKey).add(new DataTuple<Tuple>(operatorInstanceInfo, destChannel, 1, nextTuple));
                        } else { //switching: raw >> imdg
                            outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                            collector.collect(new DataTuple<Tuple>(operatorInstanceInfo, destChannel, 1, nextTuple));
                        }
                    } else {
                        if(outputQueues.containsKey(queueKey)) { // switching: imdg >> raw
                            outputQueues.remove(queueKey).add(new DataTuple<Tuple>(operatorInstanceInfo, destChannel, 0, nextTuple));
                        } else { //raw
                            collector.collect(new DataTuple<Tuple>(operatorInstanceInfo, destChannel, 0, nextTuple));
                        }
                    }
                }
            }
        }

        //check operator status. operator can be deactivated/paused if the input and output channels are IMDG
        if(paused){
//                System.out.printf("[%d] %s\n", getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "paused");
            synchronized (operatorMeta) {
                operatorMeta.wait();
            }
//                System.out.printf("[%d] %s\n", getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "resumed");
        }
    }

    @Override
    public void processElement(StreamEvent input, ProcessFunction<StreamEvent, StreamEvent>.Context context, Collector<StreamEvent> collector) throws Exception {
        this.collector = collector;
        processInputEvent(input);
    }

    public Ignite getIgniteClient() {
        return ignite;
    }

    public InputStream getDefaultInputStreams(int index) {
        return defaultInputStreams[index];
    }

    public boolean isDebug() {
        return debug;
    }

    private class QueueListenerThread extends Thread {

        private final IgniteQueue<StreamEvent> inputQueue;

        public QueueListenerThread(IgniteQueue<StreamEvent> inputQueue) {
            this.inputQueue = inputQueue;
        }

        @Override
        public void run() {
            StreamEvent event;
            do {
                event = inputQueue.take();
                if(event instanceof ActivatorEvent) {
                    if(event.getSourceInstanceInfo().equals(operatorInstanceInfo)) {
                        break;
                    }
                }
                try {
                    processInputEvent(event);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } while (true);
        }
    }
}
