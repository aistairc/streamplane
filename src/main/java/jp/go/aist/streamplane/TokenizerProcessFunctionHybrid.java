package jp.go.aist.streamplane;

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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TokenizerProcessFunctionHybrid extends ProcessFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>> {

    private String defaultInputStreamId;
    private final OutputStream[] outputStreams;
    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> outputQueues;
    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> inputQueues;

    private final Map<String, String> operatorMeta;
    private final Map<String, Map<String, String>> outputChannelMetas;

    private Ignite ignite;

    public TokenizerProcessFunctionHybrid(String defaultInputStreamId, OutputStream... outputStreams){
        this.defaultInputStreamId = defaultInputStreamId;
        this.outputStreams = outputStreams;
        this.inputQueues = new ConcurrentHashMap<>();
        this.outputQueues = new ConcurrentHashMap<>();
        this.operatorMeta = new ConcurrentHashMap<>();
        this.outputChannelMetas = new ConcurrentHashMap<>();
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

        String outputStreamIds = Arrays.stream(outputStreams).map(c -> c.getId()).collect(Collectors.joining(","));
        operatorMetaCache.put("output-stream", outputStreamIds);
        operatorMeta.put("output-stream", outputStreamIds);

        createContinousQuery(operatorMetaCacheKey, operatorMeta); //cache_id: <job_id>-task-<this_task_name>

        for(OutputStream outputStream : outputStreams) {
            String key = outputStream.getId();
            createContinousQuery(key, outputChannelMetas.computeIfAbsent(key, k -> new ConcurrentHashMap<>()));
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
//                                System.out.println("New entry: " + e.getKey() + ": " + e.getValue());
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
    public void processElement(Tuple3<Integer, Integer, String> input, ProcessFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>>.Context context, Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {
        Integer channelIndex = input.f0;
        Integer channelType = input.f1;
        String sentence = input.f2;
        do {
            if(channelIndex != this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()){
                throw new Exception("Channel index of received tuples does not match this subtask");
            }

            String[] words = sentence.toLowerCase().split("\\W+");
            for (String word : words) {
                for(OutputStream outputStream : outputStreams){
                    Integer destChannel = outputStream.getNextChannelToSendTo(word);
                    String outputStreamId = outputStream.getId();
                    String queueKey = outputStreamId + "-" + destChannel; //<stream_id>-<dest_channel>
                    if(outputChannelMetas.containsKey(outputStreamId)){
                        Map<String, String> channelMeta = outputChannelMetas.get(outputStreamId);
                        if (channelMeta.containsKey(destChannel.toString())) {
                            if(outputQueues.containsKey(queueKey)) {
                                IgniteQueue<Tuple3<Integer, Integer, String>> queue = outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                                queue.add(Tuple3.of(destChannel, 1, word));
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
            if(operatorMeta.getOrDefault("instance-status-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "Running").equals("Paused")){
//                System.out.printf("[%d] %s\n", getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "paused");
                synchronized (operatorMeta) {
                    operatorMeta.wait();
                }
//                System.out.printf("[%d] %s\n", getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "resumed");
            }

            //get next tuple
            String inputStreamKey = operatorMeta.getOrDefault("input-stream", this.defaultInputStreamId);
            String inputQueueKey = inputStreamKey + "-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            if (channelType == 1) { // imdg
                IgniteQueue<Tuple3<Integer, Integer, String>> queue = inputQueues.computeIfAbsent(inputQueueKey, k -> ignite.queue(inputQueueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                Tuple3<Integer, Integer, String> tuple = queue.take();
                channelIndex = tuple.f0;
                channelType = tuple.f1;
                sentence = tuple.f2;
                continue;
            } else {
                if(inputQueues.containsKey(inputQueueKey)) {
                    IgniteQueue<Tuple3<Integer, Integer, String>> queue = inputQueues.get(inputQueueKey);
                    if (!queue.isEmpty()) { //process inputs in IMDG if exists
                        Tuple3<Integer, Integer, String> tuple = queue.take();
                        channelIndex = tuple.f0;
                        channelType = tuple.f1;
                        sentence = tuple.f2;
                        continue;
                    } else {
                        inputQueues.remove(inputQueueKey).close();
                    }
                }
            }
        } while (channelType == 1);
    }
}
