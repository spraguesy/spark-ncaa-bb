<h1>Introduction</h1>

Here are a few examples of using Apache Spark to analyze field goal shooting with the <a href="https://console.cloud.google.com/bigquery?project=apache-spark-flatiron&folder&organizationId=797219882066&p=bigquery-public-data&d=ncaa_basketball&t=mbb_pbp_sr&page=table">NCAA-pbp</a> (play-by-play) dataset.
One examples uses mostly transformation and reading from CSV files, but it has examples of how to read from PostgreSQL
as well. The dataset is available from BigQuery. I am including one file for you to use, so you will have to get the full
dataset on your own and point the scripts to reading from their proper locations.

<h3>Apache Kafka</h3>

There is another example that sets up a Kafka Stream and the job reads from the stream and writes another stream to
the console. It also uses the kafka library in python to create a producer. This example will not run inside of Jupyter,
like the other examples, so you will need to run it from the command line.
