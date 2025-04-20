
# StreamPlane

Enable seamless cross-cluster reconfiguration of stream processing by leveranging distributed in-memory cluster to buffer in-flight data during reconfiguration. Cuerrently implemented in several mainstream streaming aplications on top of Apache Flink and Apache Ignite.

## Run Locally

### IMDG Cluster
To run locally, setup an Ignite cluser using the following docker command:

```bash
docker run --name ignite -d -p 10800:10800 -p 47100:47100 -p 47500:47500 apacheignite/ignite
```

### Operator Migration
Clone this repository and run WordCountHybrid class as the first job. Find the tokenizer_output_id and counter_output_id in the printed output.

Set input-stream and output-stream values in WordCountHybrid_2 class with the tokenizer_output_id and counter_output_id from the first job, respectively, and run the WordCountHybrid_2 class.

## Cluster Deployment

Change this statement:
```bash
final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
```
to:
```bash
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```
in each main class, and compile and package into jar file. Follow the instruction from Apache Flink documentation on how to setup Flink cluster and deploy the jobs.

## Web Interface

Interactive web interface to orchestrate the job topology available in this repository: https://github.com/aistairc/streamplane-web-interface

## Disclaimer

This is aan early access demo version that is still in development. Some variables must be manually set to make the system run. Further improvement will be updated.
