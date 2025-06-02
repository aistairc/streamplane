/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.go.aist.streamplane.examples.wordcount;

import jp.go.aist.streamplane.ImdgConfig;
import jp.go.aist.streamplane.operators.DynamicProcessFunction;
import jp.go.aist.streamplane.operators.DynamicSinkFunction;
import jp.go.aist.streamplane.operators.DynamicSourceFunction;
import jp.go.aist.streamplane.stream.InputStream;
import jp.go.aist.streamplane.events.StreamEvent;
import jp.go.aist.streamplane.stream.OutputStream;
import jp.go.aist.streamplane.stream.partitioners.StreamPlaneDefaultChannelPartitioner;
import jp.go.aist.streamplane.stream.partitioners.StreamPlaneForwardPartitioner;
import jp.go.aist.streamplane.stream.partitioners.StreamPlaneHashPartitioner;
import jp.go.aist.streamplane.stream.partitioners.StreamPlaneRebalancePartitioner;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class WordCount {

	public static void main(String[] args) throws Exception {

		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		Configuration conf = new Configuration();
		conf.setInteger("taskmanager.numberOfTaskSlots", 1);
		conf.setInteger("local.number-taskmanager", 4); // for testing more than 1 task manager
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		final ParameterTool params = ParameterTool.fromArgs(args);
		final int p = params.getInt("parallelism", env.getParallelism());
		final boolean pausedJob = params.getBoolean("paused", false);

		//for kafka
//		final String topic = params.get("topic", "t9");
//		Properties producerProps = new Properties();
//		producerProps.put("transaction.timeout.ms", 1000*60*5+"");

		OutputStream sourceOutput = new OutputStream(p, new StreamPlaneRebalancePartitioner());

		DataStream<StreamEvent> source = env
				.addSource(new DynamicSourceFunction<Tuple1<String>>(sourceOutput, pausedJob) {

					@Override
					public Tuple1<String> generateNextTuple(Long index) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return Tuple1.of(WordCountData.WORDS[Math.toIntExact(index % WordCountData.WORDS.length)]);
					}
				})
				.setParallelism(1)
				.slotSharingGroup("Non-Migratable");

		OutputStream tokenizerOutput = new OutputStream(p, new StreamPlaneHashPartitioner<String>(), 0);

		DataStream<StreamEvent> tokenizer = source
				.partitionCustom(new StreamPlaneDefaultChannelPartitioner(), StreamEvent::getDestinationInstanceIndex)
				.process(new DynamicProcessFunction<Tuple1<String>, Tuple1<String>>(
						new InputStream[]{new InputStream(sourceOutput.getId())},
						tokenizerOutput) {

					@Override
					public List<Tuple1<String>> processDataTuple(Tuple1<String> input) {
						return Arrays.stream(input.f0.toLowerCase().split("\\W+")).map(word -> Tuple1.of(word)).collect(Collectors.toList());
					}
				})
				.name("Tokenizer")
				.setParallelism(p)
				.slotSharingGroup("Non-Migratable");

		OutputStream counterOutput = new OutputStream(p, new StreamPlaneForwardPartitioner());


		DataStream<StreamEvent> counter = tokenizer
				.partitionCustom(new StreamPlaneDefaultChannelPartitioner(), StreamEvent::getDestinationInstanceIndex)
				.process(new DynamicProcessFunction<Tuple1<String>, Tuple2<String, Long>>(
						new InputStream[]{new InputStream(tokenizerOutput.getId())},
						counterOutput
				) {

					private final Map<String, IgniteAtomicLong> counters = new ConcurrentHashMap<>(); //state

					@Override
					public List<Tuple2<String, Long>> processDataTuple(Tuple1<String> input) {
						Long count = counters.computeIfAbsent(input.f0, k -> getIgniteClient().atomicLong(input.f0, 0, true)).incrementAndGet();
						return List.of(new Tuple2<>(input.f0, count));
					}

				})
				.name("Counter")
				.setParallelism(p)
				.slotSharingGroup("Migratable");

		DataStreamSink <StreamEvent> sink = counter
				.partitionCustom(new StreamPlaneDefaultChannelPartitioner(), StreamEvent::getDestinationInstanceIndex)
				.addSink(new DynamicSinkFunction<Tuple2<String, Long>>(
						new InputStream[]{new InputStream(counterOutput.getId())}
				) {

					@Override
					public void processDataTuple(Tuple2<String, Long> input) {
						System.out.printf("[%s] Message: %s\n",
								getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks(),
								input.f0 + ": " + input.f1);
					}
				})
				.name("Sink")
				.setParallelism(p)
				.slotSharingGroup("Non-Migratable");

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
				String jobId = jobClient.getJobID().toString();
				System.out.println("Job id 1: " + jobId);
				Ignite ignite = Ignition.getOrStart(ImdgConfig.CONFIG());

				//testing: instance-status
//				try {
//					Thread.sleep(10000);
//				} catch (InterruptedException e) {
//					throw new RuntimeException(e);
//				}
//
//
//				IgniteCache<String, String> tokenizerOperatorCache = ignite.getOrCreateCache(jobId + "-task-Tokenizer");
//				tokenizerOperatorCache.put("instance-status-0", "Paused"); //<instance_index>,<status>
//				tokenizerOperatorCache.put("instance-status-1", "Paused"); //<instance_index>,<status>
//
//				try {
//					Thread.sleep(10000);
//				} catch (InterruptedException e) {
//					throw new RuntimeException(e);
//				}
//
//				tokenizerOperatorCache.put("instance-status-0", "Running"); //<instance_index>,<status>
//				tokenizerOperatorCache.put("instance-status-1", "Running"); //<instance_index>,<status>
//
//				try {
//					Thread.sleep(5000);
//				} catch (InterruptedException e) {
//					throw new RuntimeException(e);
//				}

				// Switch channels from raw to imdg
//				IgniteCache<String, String> sourceOutputMetaCache = ignite.getOrCreateCache(sourceOutput.getId());
//				sourceOutputMetaCache.putIfAbsent("0", sourceOutput.getId() + "-0");
//				sourceOutputMetaCache.putIfAbsent("1", sourceOutput.getId() + "-1");
//
//				IgniteCache<String, String> tokenizerOutputMetaCache = ignite.getOrCreateCache(tokenizerOutput.getId());
//				tokenizerOutputMetaCache.putIfAbsent("0", tokenizerOutput.getId() + "-0");
//				tokenizerOutputMetaCache.putIfAbsent("1", tokenizerOutput.getId() + "-1");
//
//				IgniteCache<String, String> counterOutputMetaCache = ignite.getOrCreateCache(counterOutput.getId());
//				counterOutputMetaCache.putIfAbsent("0", counterOutput.getId() + "-0");
//				counterOutputMetaCache.putIfAbsent("1", counterOutput.getId() + "-1");
//
//                try {
//                    Thread.sleep(10000);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//
//                // Switch back channels from imdg to raw
//				sourceOutputMetaCache.remove("0");
//				sourceOutputMetaCache.remove("1");
//
//				tokenizerOutputMetaCache.remove("0");
//				tokenizerOutputMetaCache.remove("1");
//
//				counterOutputMetaCache.remove("0");
//				counterOutputMetaCache.remove("1");



				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
//
//				//testing: migrating operator instance
//				//example: migrating Counter instance (index: 1)
//				//1. change input and output channels to IMDG
				IgniteCache<String, String> tokenizerOutputMetaCache = ignite.getOrCreateCache(tokenizerOutput.getId());
				tokenizerOutputMetaCache.putIfAbsent("1", tokenizerOutput.getId() + "-1");
				IgniteCache<String, String> counterOutputMetaCache = ignite.getOrCreateCache(counterOutput.getId());
				counterOutputMetaCache.putIfAbsent("1", counterOutput.getId() + "-1");
//
//				//add delay to reflect changes
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

//				//2. Pause old instance
				IgniteCache<String, String> counterOperatorCache = ignite.getOrCreateCache(jobId + "-task-Counter");
				counterOperatorCache.put("instance-status-1", "Paused"); //<instance_index>,<status>

				//3. Update second job with the following parameters:
				System.out.println("Tokenizer output id: " + tokenizerOutput.getId());
				System.out.println("Counter output id: " + counterOutput.getId());


			}

			@Override
			public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {

			}
		});

		env.execute("WordCount on StreamPlane");

	}

}
