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

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SourceGeneratorFunctionHybrid extends RichSourceFunction<Tuple3<Integer, Integer, String>> {

    private final OutputStream[] outputStreams;
    private final AtomicInteger indexIterator;
    private final Map<String, IgniteQueue<Tuple3<Integer, Integer, String>>> outputQueues;

    private final Map<String, String> operatorMeta; // status: running/paused,
    private final Map<String, Map<String, String>> outputChannelMetas;

    private Ignite ignite;

    public SourceGeneratorFunctionHybrid(OutputStream... outputStreams) {
        this.outputStreams = outputStreams;
        this.indexIterator = new AtomicInteger(0);
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
    public void run(SourceContext<Tuple3<Integer, Integer, String>> sourceContext) throws Exception {
        int nextIndex = Math.abs(indexIterator.getAndIncrement());
        String nextSentence = WordCountData.WORDS[nextIndex % WordCountData.WORDS.length];
        while (true){
            for(OutputStream outputStream : outputStreams){
                Integer destChannel = outputStream.getNextChannelToSendTo(nextIndex);
                String prefix = outputStream.getId();
                String queueKey = prefix + "-" + destChannel; //<stream_id>-<dest_channel>
                if(outputChannelMetas.containsKey(prefix)){
                    Map<String, String> channelMeta = outputChannelMetas.get(prefix);
                    if (channelMeta.containsKey(destChannel.toString())) {
                        if(outputQueues.containsKey(queueKey)) {
                            IgniteQueue<Tuple3<Integer, Integer, String>> queue = outputQueues.computeIfAbsent(queueKey, k -> ignite.queue(queueKey, 0, ImdgConfig.QUEUE_CONFIG()));
                            queue.add(Tuple3.of(destChannel, 1, nextSentence));
                        } else { //switching: raw >> imdg
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
                } else {
                    if(outputQueues.containsKey(queueKey)) { // switching: imdg >> raw
                        outputQueues.remove(queueKey).add(Tuple3.of(destChannel, 0, nextSentence));
                    } else {
                        sourceContext.collect(Tuple3.of(destChannel, 0, nextSentence));
                    }
                }
            }

            if(operatorMeta.getOrDefault("instance-status-" + this.getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), "Running").equals("Paused")){
                synchronized (operatorMeta) {
                    operatorMeta.wait();
                }
            }

                        Thread.sleep(1000);
            nextIndex = Math.abs(indexIterator.getAndIncrement());
            nextSentence = WordCountData.WORDS[nextIndex % WordCountData.WORDS.length];
        }
    }

    @Override
    public void cancel() {

    }
}
