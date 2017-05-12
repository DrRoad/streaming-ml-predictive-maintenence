# StreamingMLPredictiveMaintenence

Example for using DSE Streaming analytics for predictive maintenence 

## Startup Script

This Asset leverages
[simple-startup](https://github.com/jshook/simple-startup). To start the entire
asset run `./startup all` for other options run `./startup`

## Manual Usage:

### Prep the files:

Make sure dsefs is turned on `dse.yaml` 

    dsefs_option
        enabled: true

And push the raw data file into the root directory of dsefs:

```
dse fs / > put file:///maintenance_data.csv maintenance_data.csv
dse fs / > ls sales_observations
maintenance_data.csv
```

### Streaming Job:
To run this on your local machine, you need to first run a Netcat server

    $ nc -lk 9999

Build:

    mvn package

and then run the example:

    $ dse spark-submit --deploy-mode cluster --supervise  --class
    com.datastax.powertools.analytics.SparkMLPredictiveMaintenenceStreamingJob
    ./target/StreamingMLPredictiveMaintenence-0.1.jar localhost 9999

To run the  model, predict via streaming, and serve results via JDBC, run the
ServeJDBC class

    $ dse spark-submit --class
    com.datastax.powertools.analytics.SparkMLPredictiveMaintenenceServeJDBC
    ./target/StreamingMLPredictiveMaintenence-0.1.jar localhost 9999


    $ dse beeline

    > !connect jdbc:hive2://localhost:10000

    > select * from recommendations.predictions where user=10277 order by prediction desc;


Into the `nc` prompt paste a few records and see the change in beeline:

```
29,0,140.3729067,104.5390305,82.25351215,TeamA,Provider1,TeamA-Provider1
58,0,95.30752801,97.49013516,102.3134457,TeamA,Provider3,TeamA-Provider3
66,1,76.76990774,110.1044455,76.71751602,TeamB,Provider3,TeamB-Provider3
36,0,119.0339468,120.1119323,70.32511371,TeamA,Provider1,TeamA-Provider1
88,1,113.7665009,88.42367648,110.0930537,TeamB,Provider4,TeamB-Provider4
67,0,109.7687115,93.37811705,122.1599585,TeamB,Provider2,TeamB-Provider2
45,0,125.7018715,101.9967219,58.30095817,TeamA,Provider2,TeamA-Provider2
30,0,93.88255716,103.1290149,110.2320489,TeamB,Provider3,TeamB-Provider3
65,1,86.98757582,123.6561241,106.6508842,TeamA,Provider3,TeamA-Provider3
76,0,87.85255835,94.28668724,114.1031517,TeamC,Provider2,TeamC-Provider2
```

Alternatively, you can run `./socketstream` to write a record per second to the stream from bash


### Docs

pull in your submodules

    git submodule update --init
    git submodule sync

then run the server

    hugo server ./content

