package jp.go.aist.streamplane;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import java.util.UUID;

public class Test {

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        Integer p = Integer.valueOf(args[0]);
        String jobId1 = args[1];
        String tokenizerOutputId = args[2];
        String counterOutputId = args[3];
        String jobId2 = args[4];

        Ignite ignite = Ignition.getOrStart(ImdgConfig.CONFIG());

        //job 1
        IgniteCache<String, String> tokenizerOutputMetaCache = ignite.getOrCreateCache(tokenizerOutputId);
        for (Integer i = 0; i < p; i++) {
            tokenizerOutputMetaCache.putIfAbsent(i.toString(), tokenizerOutputId + "-" + i);
        }
        IgniteCache<String, String> counterOutputMetaCache = ignite.getOrCreateCache(counterOutputId);
        for (Integer i = 0; i < p; i++) {
            counterOutputMetaCache.putIfAbsent(i.toString(), counterOutputId + "-" + i);
        }
        IgniteCache<String, String> counterOperatorCache = ignite.getOrCreateCache(jobId1 + "-task-Counter");
        for (Integer i = 0; i < p; i++) {
            counterOperatorCache.put("instance-status-" + i, "Paused"); //<instance_index>,<status>
        }

        //job 2
        IgniteCache<String, String> counterOperatorCache2 = ignite.getOrCreateCache(jobId2 + "-task-Counter");
        for (Integer i = 0; i < p; i++) {
            counterOperatorCache2.put("input-stream-" + i, tokenizerOutputId);
            counterOperatorCache2.put("output-stream-" + i, counterOutputId);
            counterOperatorCache2.put("instance-status-" + i, "Running");
        }

    }

}
