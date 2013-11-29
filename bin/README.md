Luigi based experimentation scripts
===================================
try on of these:
    python runOkapi.py PrepareMovielensData --fraction 0.1 --local-scheduler 
    python runOkapi.py OkapiTrainModelTask --local-scheduler --model-name Pop --in-hdfs movielens.training --out-hdfs Pop_eval