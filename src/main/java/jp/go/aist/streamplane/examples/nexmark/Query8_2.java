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

package jp.go.aist.streamplane.examples.nexmark;

import jp.go.aist.streamplane.events.StreamEvent;
import jp.go.aist.streamplane.imdg.ImdgConfig;
import jp.go.aist.streamplane.operators.DynamicProcessFunction;
import jp.go.aist.streamplane.operators.DynamicSinkFunction;
import jp.go.aist.streamplane.operators.DynamicSourceFunction;
import jp.go.aist.streamplane.stream.InputStream;
import jp.go.aist.streamplane.stream.OutputStream;
import jp.go.aist.streamplane.stream.partitioners.StreamPlaneDefaultChannelPartitioner;
import jp.go.aist.streamplane.stream.partitioners.StreamPlaneForwardPartitioner;
import jp.go.aist.streamplane.stream.partitioners.StreamPlaneHashPartitioner;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
public class Query8_2 {

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
		final boolean pausedJob = params.getBoolean("paused", true);

		//for kafka
//		final String topic = params.get("topic", "t9");
//		Properties producerProps = new Properties();
//		producerProps.put("transaction.timeout.ms", 1000*60*5+"");

		OutputStream auctionSourceOutput = new OutputStream(p, new StreamPlaneHashPartitioner<Long>(), 0);

		DataStream<StreamEvent> auctionSource = env
				.addSource(new DynamicSourceFunction<Tuple2<Long, Auction>>(auctionSourceOutput, pausedJob) {

					private GeneratorConfig config = null;
					private Long eventsCountSoFar = null;
					private final Long rate = 100L;
					private Long emitStartTime = null;
					private Long rateCounter =  null;

					private final String EMIT_START_TIME = "EMIT_START_TIME";
					private final String RATE_COUNTER = "RATE_COUNTER";
					private final String EVENT_COUNTER = "EVENT_COUNTER";

					@Override
					public Tuple2<Long, Auction> generateNextTuple(Long index) {
						if(eventsCountSoFar == null) {
							eventsCountSoFar = (Long) getState(EVENT_COUNTER);
							if(eventsCountSoFar == null) {
								eventsCountSoFar = (Long) putState(EVENT_COUNTER, 0L);
							}
						}
						if(rateCounter == null) {
							rateCounter = (Long) getState(RATE_COUNTER);
							if(rateCounter == null) {
								rateCounter = (Long) putState(RATE_COUNTER, 0L);
							}
						}
						if (config == null) {
							final NexmarkConfiguration nexmarkConfiguration = NexmarkConfiguration.DEFAULT;
							nexmarkConfiguration.hotAuctionRatio = 1;
							nexmarkConfiguration.hotSellersRatio = 1;
							nexmarkConfiguration.hotBiddersRatio = 1;
							config = new GeneratorConfig(nexmarkConfiguration, 1, 1000L + eventsCountSoFar, 0, 1);
						}
						if(eventsCountSoFar < 70_000_000) {
							if(emitStartTime == null) {
								emitStartTime = (Long) getState(EMIT_START_TIME);
							}
							if(emitStartTime == null || emitStartTime == 0L) {
								emitStartTime = (Long) putState(EMIT_START_TIME, System.currentTimeMillis());
							}
							long nextId = nextId();
							Random rnd = new Random(nextId);
							long eventTimestamp =
									config.timestampAndInterEventDelayUsForEvent(
											config.nextEventNumber(eventsCountSoFar)).getKey();

							Auction auction = AuctionGenerator.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config);
							eventsCountSoFar = (Long) putState(EVENT_COUNTER, eventsCountSoFar + 1);
							rateCounter = (Long) putState(RATE_COUNTER, rateCounter + 1);
							if(rateCounter > rate) {
								long emitTime = System.currentTimeMillis() - emitStartTime;
								if (emitTime < 1000) {
									try {
										Thread.sleep(1000 - emitTime);
									} catch (InterruptedException e) {
										throw new RuntimeException(e);
									}
								}
								emitStartTime = (Long) putState(EMIT_START_TIME, 0L);
								rateCounter = (Long) putState(RATE_COUNTER, 0L);
							}
							return Tuple2.of(auction.seller, auction);
						}
						return null;
					}

					private long nextId() {
						return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
					}
				})
				.name("AuctionSource")
				.setParallelism(1)
				.slotSharingGroup("Non-Migratable");

		OutputStream personSourceOutput = new OutputStream(p, new StreamPlaneHashPartitioner<Long>(), 0);

		DataStream<StreamEvent> personSource = env
				.addSource(new DynamicSourceFunction<Tuple2<Long, Person>>(personSourceOutput, pausedJob) {

					private GeneratorConfig config = null;
					private Long eventsCountSoFar = null;
					private final Long rate = 50L;
					private Long emitStartTime = null;
					private Long rateCounter =  null;

					private final String EMIT_START_TIME = "EMIT_START_TIME";
					private final String RATE_COUNTER = "RATE_COUNTER";
					private final String EVENT_COUNTER = "EVENT_COUNTER";

					@Override
					public Tuple2<Long, Person> generateNextTuple(Long index) {
						if(eventsCountSoFar == null) {
							eventsCountSoFar = (Long) getState(EVENT_COUNTER);
							if(eventsCountSoFar == null) {
								eventsCountSoFar = (Long) putState(EVENT_COUNTER, 0L);
							}
						}
						if(rateCounter == null) {
							rateCounter = (Long) getState(RATE_COUNTER);
							if(rateCounter == null) {
								rateCounter = (Long) putState(RATE_COUNTER, 0L);
							}
						}
						if (config == null) {
							final NexmarkConfiguration nexmarkConfiguration = NexmarkConfiguration.DEFAULT;
							nexmarkConfiguration.hotAuctionRatio = 1;
							nexmarkConfiguration.hotSellersRatio = 1;
							nexmarkConfiguration.hotBiddersRatio = 1;
							config = new GeneratorConfig(nexmarkConfiguration, 1, 1000L + eventsCountSoFar, 0, 1);
						}
						if(eventsCountSoFar < 40_000_000) {
							if(emitStartTime == null) {
								emitStartTime = (Long) getState(EMIT_START_TIME);
							}
							if(emitStartTime == null || emitStartTime == 0L) {
								emitStartTime = (Long) putState(EMIT_START_TIME, System.currentTimeMillis());
							}
							long nextId = nextId();
							Random rnd = new Random(nextId);
							long eventTimestamp =
									config.timestampAndInterEventDelayUsForEvent(
											config.nextEventNumber(eventsCountSoFar)).getKey();

							Person person = PersonGenerator.nextPerson(nextId, rnd, new DateTime(eventTimestamp), config);
							eventsCountSoFar = (Long) putState(EVENT_COUNTER, eventsCountSoFar + 1);
							rateCounter = (Long) putState(RATE_COUNTER, rateCounter + 1);
							if(rateCounter > rate) {
								long emitTime = System.currentTimeMillis() - emitStartTime;
								if (emitTime < 1000) {
									try {
										Thread.sleep(1000 - emitTime);
									} catch (InterruptedException e) {
										throw new RuntimeException(e);
									}
								}
								emitStartTime = (Long) putState(EMIT_START_TIME, 0L);
								rateCounter = (Long) putState(RATE_COUNTER, 0L);
							}
							return Tuple2.of(person.id, person);
						}
						return null;
					}

					private long nextId() {
						return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
					}
				})
				.name("PersonSource")
				.setParallelism(1)
				.slotSharingGroup("Non-Migratable");


		OutputStream joinedStreamOutput = new OutputStream(p, new StreamPlaneForwardPartitioner());

		DataStream<StreamEvent> joinedStream = auctionSource.union(personSource)
				.partitionCustom(new StreamPlaneDefaultChannelPartitioner(), StreamEvent::getDestinationInstanceIndex)
				.process(new DynamicProcessFunction<Tuple2<Long, KnownSize>, Tuple3<Long, String, Long>>(
						new InputStream[]{new InputStream(auctionSourceOutput.getId(), 0), new InputStream(personSourceOutput.getId(), 0)},
						joinedStreamOutput,
						pausedJob
				) {
					private Long currentWindowStartTime = null;
					private IgniteQueue<Tuple2<Long, Auction>> auctionSet = null;
					private IgniteQueue<Tuple2<Long, Person>> personSet = null;

					private final String CURRENT_WINDOW_START_TIME = "CURRENT_WINDOW_START_TIME";

					@Override
					public List<Tuple3<Long, String, Long>> processDataTuple(Tuple2<Long, KnownSize> input) {
						Auction auction = null;
						Person person = null;
						if(input.f1 instanceof Auction) {
							auction = (Auction) input.f1;
						}
						if(input.f1 instanceof Person) {
							person = (Person) input.f1;
						}
						if(auctionSet == null) {
							auctionSet = getIgniteClient().queue(getStateIdPrefix() + "-auction_set", 0, ImdgConfig.QUEUE_CONFIG());
							auctionSet.clear();
						}
						if(personSet == null) {
							personSet = getIgniteClient().queue(getStateIdPrefix() + "-person_set", 0, ImdgConfig.QUEUE_CONFIG());
							personSet.clear();
						}
						//calculate tumbling event time window
						if(currentWindowStartTime == null) {
							currentWindowStartTime = (Long) getState(CURRENT_WINDOW_START_TIME);
						}
						if(currentWindowStartTime == null || currentWindowStartTime == 0L) {
							if(auction != null) {
								currentWindowStartTime = (Long) putState(CURRENT_WINDOW_START_TIME, auction.dateTime.getMillis());
							} else {
								currentWindowStartTime = (Long) putState(CURRENT_WINDOW_START_TIME, person.dateTime.getMillis());
							}
						}

						long currentEventTime = auction != null ? auction.dateTime.getMillis() : person.dateTime.getMillis();

						long interval = currentEventTime - currentWindowStartTime;
						if(interval >= 1000){ //TumblingEventTimeWindows.of(Time.seconds(10))
							List<Tuple3<Long, String, Long>> result = new ArrayList<>();
							for(Tuple2<Long, Auction> auctionEvent : auctionSet) {
								for(Tuple2<Long, Person> personEvent : personSet) {
									System.out.printf("Auction: %s, Person: %s\n", auctionEvent.f0, personEvent.f0);
									if(auctionEvent.getField(getDefaultInputStreams(0).getKeyFieldIndex())
											.equals(personEvent.getField(getDefaultInputStreams(1).getKeyFieldIndex()))) {
										result.add(Tuple3.of(personEvent.f1.id, personEvent.f1.name, auctionEvent.f1.reserve));
									}
								}
							}
							currentWindowStartTime = (Long) putState(CURRENT_WINDOW_START_TIME, 0L);
							auctionSet.clear();
							personSet.clear();
							return result;
						} else {
							if(input.f1 instanceof Auction) {
								auctionSet.add(Tuple2.of(input.f0, (Auction) input.f1));
							}
							if(input.f1 instanceof Person) {
								personSet.add(Tuple2.of(input.f0, (Person) input.f1));
							}
							return List.of();
						}
					}
				})
				.name("JoinedStream")
				.setParallelism(p)
				.slotSharingGroup("Migratable");

		DataStreamSink <StreamEvent> sink = joinedStream
				.partitionCustom(new StreamPlaneDefaultChannelPartitioner(), StreamEvent::getDestinationInstanceIndex)
				.addSink(new DynamicSinkFunction<Tuple3<Long, String, Long>>(
						new InputStream[]{new InputStream(joinedStreamOutput.getId())},
						pausedJob
				) {

					@Override
					public void processDataTuple(Tuple3<Long, String, Long> input) {
						System.out.printf("[%s] Message: %s\n",
								getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks(),
								input.f0 + "," + input.f1 + "," + input.f2);
					}

				})
				.name("Sink")
				.setParallelism(p)
				.slotSharingGroup("Non-Migratable");

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
				String jobId = jobClient.getJobID().toString();
				System.out.println("Job id 2: " + jobId);
				Ignite ignite = Ignition.getOrStart(ImdgConfig.CONFIG());

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				//testing: migrating operator instance in second job
				//example: migrating Join operator instance (index: 1)
				//1. change input-stream of Join to IMDG from first job
				IgniteCache<String, String> joinedOperatorCache = ignite.getOrCreateCache(jobId + "-task-JoinedStream");
				joinedOperatorCache.put("input-stream-1", "22592603-cd96-4414-acfd-a4d317c7af81,b4f16bc3-584c-4f69-bab8-ef783a7f1154");

				//2. change output-stream of Join to first job's Join's output
				joinedOperatorCache.put("output-stream-1", "744164a2-3659-49b6-9567-0a77a2ffdd0b");

				//3. Resume processing
				joinedOperatorCache.put("instance-status-1", "Running");
				
			}

			@Override
			public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {

			}
		});

		env.execute("Nexmark Q8 on StreamPlane");

	}

}
