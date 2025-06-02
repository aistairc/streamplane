package jp.go.aist.streamplane.examples.wordcountlegacy;

import jp.go.aist.streamplane.ImdgConfig;
import jp.go.aist.streamplane.stream.OutputStream;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple3;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class TokenizerProcessFunctionHybrid extends ProcessFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>> {

    private String defaultInputStreamId;
    private final OutputStream defaultOutputStream;
    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> outputQueues;
    private IgniteQueue<Tuple3<Integer, Integer, String>> inputQueue;

    private final Map<String, String> operatorMeta; // status: running/paused,
    private final Map<String, String> defaultOutputChannelMeta;
    private String currentOutputStreamId;
    private Map<String, String> currentOutputChannelMeta;

    private Ignite ignite;

    public TokenizerProcessFunctionHybrid(String defaultInputStreamId, OutputStream defaultOutputStream){
        this.defaultInputStreamId = defaultInputStreamId;
        this.defaultOutputStream = defaultOutputStream;
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

        String inputStream = operatorMetaCache.getAndPutIfAbsent("input-stream-" + subtaskIndex, this.defaultInputStreamId);
        if(inputStream == null){
            inputStream = this.defaultInputStreamId;
        }
        operatorMeta.put("input-stream-" + subtaskIndex, inputStream);
        inputQueue = ignite.queue(inputStream + "-" + subtaskIndex, 0, ImdgConfig.QUEUE_CONFIG());

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
                                entries.put(e.getKey(), e.getValue());
                                if(e.getKey().equals("instance-status-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()) && e.getValue().equals("Running")){
                                    synchronized (operatorMeta) {
                                        operatorMeta.notifyAll();
                                    }
                                }

                                if(e.getKey().equals("input-stream-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())){
                                    inputQueue = ignite.queue(e.getValue() + "-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), 0, ImdgConfig.QUEUE_CONFIG());
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
    public void processElement(Tuple3<Integer, Integer, String> input, ProcessFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>>.Context context, Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {
        Integer channelIndex = input.f0;
        Integer channelType = input.f1;
        String sentence = input.f2;
        do {
            if(channelType == -1) { //paused job
                if(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == 0) {
                    for (int i = 0; i < defaultOutputStream.getParallelism(); i++) {
                        collector.collect(Tuple3.of(i, -1, null));
                    }
                }
                synchronized (operatorMeta) {
                    operatorMeta.wait();
                    channelType = 1; //switch to IMDG when resumed
                }
            } else {
                if(channelIndex != this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()){
                    throw new Exception("Channel index of received tuples does not match this subtask");
                }

                String[] words = sentence.toLowerCase().split("\\W+");
                for (String word : words) {
                    Integer destChannel = defaultOutputStream.getNextChannelToSendTo(null);
                    String queueKey = currentOutputStreamId + "-" + destChannel; //<stream_id>-<dest_channel>
                    if (currentOutputChannelMeta.containsKey(destChannel.toString())) {
                        if(outputQueues.containsKey(queueKey)) {
                            outputQueues.get(queueKey).add(Tuple3.of(destChannel, 1, word));
                        } else { //switching: raw >> imdg
                            outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                            collector.collect(Tuple3.of(destChannel, 1, word));
                        }
                    } else {
                        if(outputQueues.containsKey(queueKey)) { // switching: imdg >> raw
                            outputQueues.remove(queueKey).add(Tuple3.of(destChannel, 0, word));
                        } else {
                            collector.collect(Tuple3.of(destChannel, 0, word));
                        }
                    }
                }
            }

            //check operator status. operator can be deactivated if the input and output channels are IMDG
            //Thus, deactivation make this operator stop listening inputs from IMDG channels
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            if(operatorMeta.getOrDefault("instance-status-" + subtaskIndex, "Running").equals("Paused")){
//                System.out.printf("[%d] %s\n", getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "paused");
                synchronized (operatorMeta) {
                    operatorMeta.wait();
                }
//                System.out.printf("[%d] %s\n", getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "resumed");
            }

            //get next tuple
            if (channelType == 1) { // imdg
                Tuple3<Integer, Integer, String> tuple = inputQueue.take();
                channelIndex = tuple.f0;
                channelType = tuple.f1;
                sentence = tuple.f2;
                continue;
            } else {
                if (!inputQueue.isEmpty()) { //process inputs in IMDG if exists
                    Tuple3<Integer, Integer, String> tuple = inputQueue.take();
                    channelIndex = tuple.f0;
                    channelType = tuple.f1;
                    sentence = tuple.f2;
                    continue;
                } else {
//                    inputQueue.close();
                }
            }
        } while (channelType == 1);
    }
}
