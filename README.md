# StreamPlane

Enable seamless cross-cluster reconfiguration of stream processing by leveraging a distributed in-memory cluster to buffer in-flight data during reconfiguration. Currently implemented in sample mainstream streaming applications on top of Apache Flink and Apache Ignite.

## Motivation

Existing Distributed Stream Processing Engines (DSPEs), like Apache FLink, maintain streaming applications, or streaming jobs, within a DSPE cluster. A cluster is managed by a central coordinator, which is responsible for initializing the application topology and distributing tasks to the participating nodes. Typically, DSPEs do not offer a way to reconfigure a running job on the fly. Instead, the job must be detached from the cluster and redeployed with the new configuration, either manually or automatically, such as [Adaptive Scheduler and Reactive Mode](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/elastic_scaling/). Moreover, in some cases, a single cluster is sufficient. However, there are situations in which certain operators of an application running on edge servers need to be migrated to a different cluster in the cloud. This strategy helps to meet resource demands without expanding the existing cluster, which could disrupt other applications. Additionally, it may be necessary to co-locate confidential operators in a private cloud while allowing less confidential ones to be hosted in a public cloud. StreamPlane to enable flexible and seamless control and reconfiguration of streaming job topology, leveraging In-Memory Data Grid (IMDG) as the control plane layer on top of existing DSPEs.

## Run Locally

### IMDG Cluster
To run locally, setup an Ignite cluster using the following Docker command:

```bash
docker run --name ignite -d -p 10800:10800 -p 47100:47100 -p 47500:47500 apacheignite/ignite
```

### Operator Migration
Clone this repository and run the WordCountHybrid class as the first job. Find the tokenizer_output_id and counter_output_id in the printed output.

Set input-stream and output-stream values in the WordCountHybrid_2 class with the tokenizer_output_id and counter_output_id from the first job, respectively, and run the WordCountHybrid_2 class.

## Cluster Deployment

Change this statement:
```bash
final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
```
to:
```bash
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```
In each main class, compile and package into a jar file. Follow the instructions from Apache Flink documentation on how to set up a Flink cluster and deploy the jobs.

## Web Interface

Interactive web interface to orchestrate the job topology available in this repository: https://github.com/aistairc/streamplane-web-interface

## Demo Paper

[StreamPlane: Flexible Reconfiguration of Streaming Application Runtime Topology](https://dl.acm.org/doi/10.1145/3701717.3733229)

## Disclaimer

This is an early access demo version that is still in development. Some variables must be manually set to make the system run. Further improvements will be updated.

## Acknowledgement

This software is based on results obtained from the project, "Research and Development Project of the Enhanced infrastructures for Post 5G Information and Communication Systems" JPNP 20017, commissioned by the New Energy and Industrial Technology Development Organization (NEDO).
