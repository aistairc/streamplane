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

		Properties producerProps = new Properties();
		producerProps.put("transaction.timeout.ms", 1000*60*5+"");

		OutputStream sourceOutput = new OutputStream(p, new CustomRebalancePartitioner());

		DataStream<Tuple3<Integer, Integer, String>> source = env
				.addSource(new SourceGeneratorFunctionHybrid(sourceOutput))
				.setParallelism(1);

		OutputStream tokenizerOutput = new OutputStream(p, new CustomHashPartitioner());

		DataStream<Tuple3<Integer, Integer, String>> tokenizer = source
				.partitionCustom(new ChannelPartitioner(), tuple -> tuple.f0)
				.process(new TokenizerProcessFunctionHybrid(sourceOutput.getId(), tokenizerOutput))
				.name("Tokenizer")
				.setParallelism(p);

		OutputStream counterOutput = new OutputStream(1, new CustomHashPartitioner());

		DataStream<Tuple3<Integer, Integer, String>> counter = tokenizer
				.partitionCustom(new ChannelPartitioner(), tuple -> tuple.f0)
				.process(new CounterProcessFunctionHybrid(tokenizerOutput.getId(), counterOutput))
				.name("Counter")
				.setParallelism(p);

		DataStreamSink <Tuple3<Integer, Integer, String>> sink = counter
				.partitionCustom(new ChannelPartitioner(), tuple -> tuple.f0)
				.addSink(new SinkFunctionHybrid(counterOutput.getId()))
				.name("Sink")
				.setParallelism(1);

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
				String jobId = jobClient.getJobID().toString();
				Ignite ignite = Ignition.getOrStart(ImdgConfig.CONFIG());
//testing: instance-status
/*
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
*/

				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				// Switch all channels from raw to imdg
//				IgniteCache<String, String> tokenizerChannelMetaCache = ignite.getOrCreateCache(sourceOutput.getId());
//				tokenizerChannelMetaCache.putIfAbsent("0", sourceOutput.getId() + "-0");
//				tokenizerChannelMetaCache.putIfAbsent("0", sourceOutput.getId() + "-0");


//				IgniteCache<String, String> counterChannelMetaCache = ignite.getOrCreateCache(jobId + "-Counter-OUT");
//				counterChannelMetaCache.putIfAbsent("0", jobId + "-Counter-OUT-0"); //<channel_index>,<queue_key>
//				counterChannelMetaCache.putIfAbsent("1", jobId + "-Counter-OUT-1"); //<channel_index>,<queue_key>

				IgniteCache<String, String> sinkChannelMetaCache = ignite.getOrCreateCache(counterOutput.getId());
				sinkChannelMetaCache.putIfAbsent("0", counterOutput.getId() + "-0"); //<channel_index>,<queue_key>
//				sinkChannelMetaCache.putIfAbsent("1", jobId + "-Sink: Sink-OUT-1"); //<channel_index>,<queue_key>

//                try {
//                    Thread.sleep(5000);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//				System.out.println("#### 4");
//                // Switch back all channels from imdg to raw
//				tokenizerChannelMetaCache.remove("0"); //<channel_index>,<queue_key>
//				tokenizerChannelMetaCache.remove("1"); //<channel_index>,<queue_key>
//
//				try {
//					Thread.sleep(5000);
//				} catch (InterruptedException e) {
//					throw new RuntimeException(e);
//				}
//				System.out.println("#### 5");
//				counterChannelMetaCache.remove("0"); //<channel_index>,<queue_key>
//				counterChannelMetaCache.remove("1"); //<channel_index>,<queue_key>
//
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				sinkChannelMetaCache.remove("0"); //<channel_index>,<queue_key>
			}

			@Override
			public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {

			}
		});

		env.execute("Flink Ignite");

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

	public static class ChannelPartitioner implements Partitioner<Integer> {
		@Override
		public int partition(Integer key, int numPartitions) {
			return Math.abs(key) % numPartitions;
		}
	}


}
