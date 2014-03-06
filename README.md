Okapi
========

## Overview

Okapi is a library of Machine Learning and graph mining algorithms for Giraph. The library includes state-of-the-art Collaborative Filtering algorithms used in recommendation systems as well as graph algorithms such as partitioning, clustering and sybil-detection for OSNs.

The Okapi library is developed by the Telefonica Research lab and is available as open source under the Apache License. We invite users to contribute to the library, to make it more robust and rich in features. Okapi is part of the [Grafos.ML](http://grafos.ml) project. 

For a full list of the provided algorithms, documentation, and instructions on how to use Okapi, please visit the [Grafos.ML](http://grafos.ml) page.


## Building

Although you can find pre-built packages on the [Grafos.ML](http://grafos.ml) for different Hadoop distributions, you may very likely need to build the code youself. Go into $OKAPI_HOME, the directory where you cloned the code, and run:

    mvn package

This will build the code and also run some tests. If you want to skip the tests, then run:

    mvn package -DskipTests

After that, under $OKAPI_HOME/target, you should find a jar file with a name of the type:
    
    okapi-${VERSION}-jar-with-dependencies.jar
    
Inside the jar, we package the Okapi library as well as all dependencies for convenience. 

## Running

Running an Okapi job does not differ from running an ordinary Giraph job. You can use the pre-built jars or the jar you built yourself to launch a Giraph job as described on the [Giraph](http://giraph.apache.org/) site. On our site, we also provide a web-based tool that helps you construct the command you need to execute. Check it out!

## Collaborative Filtering
To run any of the CF algorithms, we suggest to use bin/runOkapi.py, i.e.:

    python runOkapi.py SpitPrecision --local-scheduler --model-name Pop --fraction 1.0

This gives Precision@5, and uses evaluation procedure where we sample 100 items, mark them
as irrelevant and rank all the list according to the predicted scores. 
We also provide some results. These are for reference only (default parameters, no cross-validation).
Note, that many of these algorithms were not designed to run on data sets
with ratings, others optimize rating prediction, therefore, we compare apples and boxes.

Okapi-0.3.2-SNAPSHOT and Movielens 1M dataset:

--model-name          | Precision@5 
----------------------|-------------
Random                | 0.1529      
Pop                   | 0.7554      
BPR                   | ~~0.2412~~
