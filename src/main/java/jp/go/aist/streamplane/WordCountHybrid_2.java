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

package jp.go.aist.streamplane;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import javax.annotation.Nullable;
import java.util.Properties;

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
public class WordCountHybrid_2 {

	public static void main(String[] args) throws Exception {

		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		Configuration conf = new Configuration();
		conf.setInteger("taskmanager.numberOfTaskSlots", 1);
		conf.setInteger("local.number-taskmanager", 2); // for testing more than 1 task manager
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		final ParameterTool params = ParameterTool.fromArgs(args);
		final String topic = params.get("topic", "t9");
		final int p = params.getInt("p", env.getParallelism());
		final boolean pausedJob = params.getBoolean("paused", true);

		Properties producerProps = new Properties();
		producerProps.put("transaction.timeout.ms", 1000*60*5+"");

		OutputStream sourceOutput = new OutputStream(p, new CustomRebalancePartitioner());

		DataStream<Tuple3<Integer, Integer, String>> source = env
				.addSource(new SourceGeneratorFunctionHybrid(pausedJob, sourceOutput))
				.setParallelism(1);

		OutputStream tokenizerOutput = new OutputStream(p, new CustomHashPartitioner());

		DataStream<Tuple3<Integer, Integer, String>> tokenizer = source
				.partitionCustom(new ChannelPartitioner(), tuple -> tuple.f0)
				.process(new TokenizerProcessFunctionHybrid(sourceOutput.getId(), tokenizerOutput))
				.name("Tokenizer")
				.setParallelism(p);

		OutputStream counterOutput = new OutputStream(p, new WordCountHybrid.CustomForwardPartitioner());

		DataStream<Tuple3<Integer, Integer, String>> counter = tokenizer
				.partitionCustom(new ChannelPartitioner(), tuple -> tuple.f0)
				.process(new CounterProcessFunctionHybrid(tokenizerOutput.getId(), counterOutput))
				.name("Counter")
				.setParallelism(p);

		DataStreamSink <Tuple3<Integer, Integer, String>> sink = counter
				.partitionCustom(new ChannelPartitioner(), tuple -> tuple.f0)
				.addSink(new SinkFunctionHybrid(counterOutput.getId()))
				.name("Sink")
				.setParallelism(p);

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
				String jobId = jobClient.getJobID().toString();
				System.out.println("Job id 2: " + jobId);
				Ignite ignite = Ignition.getOrStart(ImdgConfig.CONFIG());
/*
				//testing: instance-status
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}


				IgniteCache<String, String> tokenizerOperatorCache = ignite.getOrCreateCache(jobId + "-task-Tokenizer");
				tokenizerOperatorCache.put("instance-status-0", "Paused"); //<instance_index>,<status>
				tokenizerOperatorCache.put("instance-status-1", "Paused"); //<instance_index>,<status>

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				tokenizerOperatorCache.put("instance-status-0", "Running"); //<instance_index>,<status>
				tokenizerOperatorCache.put("instance-status-1", "Running"); //<instance_index>,<status>

				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				// Switch channels from raw to imdg
				IgniteCache<String, String> sourceOutputMetaCache = ignite.getOrCreateCache(sourceOutput.getId());
				sourceOutputMetaCache.putIfAbsent("0", sourceOutput.getId() + "-0");
				sourceOutputMetaCache.putIfAbsent("1", sourceOutput.getId() + "-1");

				IgniteCache<String, String> tokenizerOutputMetaCache = ignite.getOrCreateCache(tokenizerOutput.getId());
				tokenizerOutputMetaCache.putIfAbsent("0", tokenizerOutput.getId() + "-0");
				tokenizerOutputMetaCache.putIfAbsent("1", tokenizerOutput.getId() + "-1");

				IgniteCache<String, String> counterOutputMetaCache = ignite.getOrCreateCache(counterOutput.getId());
				counterOutputMetaCache.putIfAbsent("0", counterOutput.getId() + "-0");

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                // Switch back channels from imdg to raw
				sourceOutputMetaCache.remove("0");
				sourceOutputMetaCache.remove("1");

				tokenizerOutputMetaCache.remove("0");
				tokenizerOutputMetaCache.remove("1");

				counterOutputMetaCache.remove("0");
 */

//				try {
//					Thread.sleep(5000);
//				} catch (InterruptedException e) {
//					throw new RuntimeException(e);
//				}

				//testing: migrating operator instance in second job
				//example: migrating Counter instance (index: 1)
				//1. change input-stream of Counter to IMDG from first job
				IgniteCache<String, String> counterOperatorCache = ignite.getOrCreateCache(jobId + "-task-Counter");
				counterOperatorCache.put("input-stream-1", "<origin_tokenizer_output_stream_id>");

				//2. change output-stream of Counter to first job's Counter's output
				counterOperatorCache.put("output-stream-1", "<origin_counter_output_stream_id>");

				//3. Resume processing
				counterOperatorCache.put("instance-status-1", "Running");

			}

			@Override
			public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {

			}
		});

		env.execute("WordCount on StreamPlane");

	}

	public static class CustomRebalancePartitioner implements Partitioner<Integer> {

		private int nextChannelToSendTo = 0;

		@Override
		public int partition(Integer key, int numPartitions) {
			this.nextChannelToSendTo = (this.nextChannelToSendTo + 1) % numPartitions;
			return this.nextChannelToSendTo;
		}
	}

	public static class CustomHashPartitioner implements Partitioner<String> {

		@Override
		public int partition(String key, int numPartitions) {
			return Math.abs(key.hashCode() % numPartitions);
		}
	}

	public static class CustomForwardPartitioner implements Partitioner<Integer> {

		@Override
		public int partition(Integer key, int numPartitions) {
			return key;
		}
	}

	public static class ChannelPartitioner implements Partitioner<Integer> {
		@Override
		public int partition(Integer key, int numPartitions) {
			return Math.abs(key) % numPartitions;
		}
	}


}
