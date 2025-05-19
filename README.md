# StreamPlane

Enable seamless cross-cluster reconfiguration of stream processing by leveraging a distributed in-memory cluster to buffer in-flight data during reconfiguration. Currently implemented in several mainstream streaming applications on top of Apache Flink and Apache Ignite.

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

## Disclaimer

This is an early access demo version that is still in development. Some variables must be manually set to make the system run. Further improvements will be updated.

## Acknowledgement

This software is based on results obtained from the project, "Research and Development Project of the Enhanced infrastructures for Post 5G Information and Communication Systems" JPNP 20017, commissioned by the New Energy and Industrial Technology Development Organization (NEDO).
