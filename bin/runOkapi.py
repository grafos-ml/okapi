'''
This is driver for the Okapi parameter tuning and experimentation.
***Edit client.cfg***

Run this script on the hadoop server. Note, that hadoop binary should be on the path.

The most useful tasks would be:
    python runOkapi.py PrepareMovielensData --fraction 0.1 --local-scheduler 
    python runOkapi.py OkapiTrainModelTask --local-scheduler --model-name Pop --in-hdfs movielens.training --out-hdfs Pop_eval
'''
from luigi.parameter import Parameter

__author__ = 'linas'

import logging, os, urllib, zipfile
import luigi, luigi.hadoop_jar, luigi.hdfs

#all available methods and theri Computation classes
methods = { 'BPR': 'ml.grafos.okapi.cf.ranking.BPRRankingComputation',
            'Pop': 'ml.grafos.okapi.cf.ranking.PopularityRankingComputation',
            'Random' : 'ml.grafos.okapi.cf.ranking.RandomRankingComputation',
            'TFMAP' : 'ml.grafos.okapi.cf.ranking.TFMAPRankingComputation'
}

logger = logging.getLogger('luigi-interface')

class DownloadMovielens(luigi.Task):
    '''Downloads the Movielens data and extracts a ratings.dat file'''
    uri = 'http://files.grouplens.org/datasets/movielens/ml-10m.zip'

    def output(self):
        return luigi.LocalTarget("ratings.dat")

    def run(self):
        name, _ = urllib.urlretrieve(self.uri, "ml-10m.zip")
        z = zipfile.ZipFile(name)
        data = z.read("ml-10M100K/ratings.dat")
        f = open("ratings.dat", 'w')
        f.write(data)
        f.close()

class PrepareMovielensData(luigi.Task):
    '''Splits the data into training, validation and testing. Reindex it according to the okapi needs'''

    fraction = luigi.Parameter(description="The fraction of data we want to use", default=1.0)

    #remaking ids for okapi: starts 1 (items -1), no gaps, no new items in the test/validation set
    training_users = {"original_id" :0}
    training_items = {"original_id": 0}

    def requires(self):
        return DownloadMovielens()

    def output(self):
        return [luigi.hdfs.HdfsTarget('movielens.testing'),
                luigi.hdfs.HdfsTarget('movielens.training'),
                luigi.hdfs.HdfsTarget('movielens.training.info'),
                luigi.hdfs.HdfsTarget('movielens.validation')]

    def _get_id(self, original_id, dictionary):
        id = dictionary.get(original_id, len(dictionary))
        dictionary[original_id] = id
        return id

    def run(self):
        '''
        1. 70% entries go to training, others go to memory
        2. from memory, items and users that are in training 33% of items go into validation, 66% go to testing
        '''
        import random
        random.seed(123)#just that all user would have the same data sets

        f = self.input().open('r') # this will return a file stream that reads from movielens ratings.dat
        training = self.output()[1].open('w')

        #lets first write training set and store in memory user and item indexes
        testing_validation = []
        cnt = 0
        for line in f:
            r = random.random()
            user,item,rating,time = line.split("::")
            rating = int(float(rating))
            if r < 0.7: #write to training
                userid = self._get_id(user, self.training_users)
                itemid = self._get_id(item, self.training_items)

                training.write("{} {} {}\n".format(userid, itemid, rating))
                cnt += 1
            else:
                testing_validation.append((user, item, rating))
        training.close() # needed because files are atomic


        #now lets write out the testing and validation
        testing = self.output()[0].open('w')
        validation = self.output()[3].open('w')
        for u,i,r in testing_validation:
            if u in self.training_users and i in self.training_items:
                r = random.random()
                if r < 0.33:
                    validation.write('{} {} {}\n'.format(self.training_users[u], self.training_items[i], rating))
                else:
                    testing.write('{} {} {}\n'.format(self.training_users[u], self.training_items[i], rating))
        testing.close()
        validation.close()
        f.close()

        info = self.output()[2].open('w')
        info.write('n_users: {}, n_items: {}, n_entries: {}\n'.format(len(self.training_users), len(self.training_items), cnt))
        info.close()

class DeleteDir(luigi.Task):
    '''Removes a given HDFS directory.'''
    dir = luigi.Parameter(description="Directory to delete")

    def run(self):
        logger.debug("Checking if {} is available".format(self.dir))
        if luigi.hdfs.exists(self.dir):
            logger.debug("Removing {}.".format(self.dir))
            luigi.hdfs.remove(self.dir)

    def complete(self):
        return not luigi.hdfs.exists(self.dir)

class OkapiTrainModelTask(luigi.hadoop_jar.HadoopJarJobTask):
    '''Trains a model'''

    model_name = luigi.Parameter(description="The model: {"+" | ".join(methods)+"}")
    in_hdfs = luigi.Parameter(description="Training input file")
    out_hdfs = luigi.Parameter(description="Output dir for the task")

    def requires(self):
        #we need to delete a special zookeeper dir because of some strange behaviour
        return PrepareMovielensData(), DeleteDir(self._get_conf("hadoop", "zookeeper-dir"))

    def output(self):
        return luigi.hdfs.HdfsTarget(self.out_hdfs)

    def _get_conf(self, section, name):
        return luigi.configuration.get_config().get(section, name)

    def get_computation_class(self):
        if self.model_name in methods:
            return methods[self.model_name]
        else:
            raise "Not implemented method. Please choose from {"+ " | ".join(methods.keys())+"}"

    def get_input_format(self):
        return 'ml.grafos.okapi.cf.CfLongIdFloatTextInputFormat'

    def get_output_format(self):
        return 'org.apache.giraph.io.formats.IdWithValueTextOutputFormat'

    def get_input(self):
        return self.in_hdfs

    def get_output(self):
        return self.out_hdfs

    def run(self):
        self.set_hadoop_classpath()
        super(OkapiTrainModelTask, self).run()

    def get_libjars(self):
        return [self.giraph_jar(), self.okapi_jar()]

    def set_hadoop_classpath(self):
        '''we need to put our jars into the classpath of the hadoop'''
        hadoop_cp = ':'.join(filter(None, self.get_libjars()))
        if os.environ.get('HADOOP_CLASSPATH', None):
            os.environ['HADOOP_CLASSPATH'] = os.environ['HADOOP_CLASSPATH']+":"+hadoop_cp
            print os.environ['HADOOP_CLASSPATH']
        else:
            os.environ['HADOOP_CLASSPATH'] = hadoop_cp
        logger.debug("HADOOP_CLASSPATH={}".format(os.environ['HADOOP_CLASSPATH']))
        print os.environ['HADOOP_CLASSPATH']

    def jar(self):
        return self.giraph_jar()

    def main(self):
        return 'org.apache.giraph.GiraphRunner'

    def get_jar(self, group, jarname):
        config = luigi.configuration.get_config()
        jar = config.get(group, jarname)
        if not jar:
            logger.error("You must specify {} in client.cfg".format(jarname))
            raise
        if not os.path.exists(jar):
            logger.error("Can't find {} jar: ".format(jarname))
            raise
        return jar

    def okapi_jar(self):
        return self.get_jar("okapi", "okapi-jar")

    def giraph_jar(self):
        return self.get_jar("okapi", "giraph-jar")

    def get_custom_arguments(self):
        return ['-ca', 'minItemId=1', '-ca', 'maxItemId=10628']

    def args(self):
        return [
            "-libjars", ",".join(self.get_libjars()),
            "-Dmapred.child.java.opts="+self._get_conf('hadoop', 'hadoop-mem'),
            "-Dgiraph.zkManagerDirectory="+self._get_conf('hadoop', 'zookeeper-dir'),
            "-Dgiraph.useSuperstepCounters=false",
            self.get_computation_class(),
            '-eif', self.get_input_format(),
            '-eip', self.get_input(),
            '-vof', self.get_output_format(),
            '-op', self.get_output(),
            '-w', self._get_conf("okapi", "workers")] \
            + self.get_custom_arguments()


# class JoinModelToTest(luigi.Task):
#
#     _user_models = None
#     _item_models = None
#
#     join_out = luigi.Parameter(description="Output dir")
#     model_name = luigi.Parameter(description="Model Name")
#
#     def output(self):
#         return luigi.hdfs.HdfsTarget(self.join_out)
#
#     def requires(self):
#         return [PrepareMovielensData(), OkapiTrainModelTask(self.model_name, 'movielens.training', self.model_name+"_model")]
#
#     def run(self):
#         if self._user_models is None or self._item_models is None:
#             self._item_models = {}
#             self._user_models = {}
#             f = luigi.hdfs.HdfsTarget(self.model_name+"_model", format=luigi.hdfs.PlainDir).open()
#             for line in f:
#                 node_id, model = line.strip().split("\t")
#                 id, tipe = node_id.split()
#                 if "0" == tipe:
#                     self._user_models[id] = model
#                 if "1" == tipe:
#                     self._item_models[id] = model
#
#         out_file = self.output().open('w')
#         in_file = self.input()[0][0].open('r')
#
#         user_in_test = set()
#         item_in_test = set()
#         out_file.write("0 -1\n")
#         for line in in_file:
#             user, item, rating = line.split()
#             if user in self._user_models and item in self._item_models:
#                 user_in_test.add(user)
#                 item_in_test.add(item)
#                 out_file.write("{} 0\t{} 1\t{}\n".format(user, item, rating))
#
#         for u in user_in_test:
#             out_file.write("{} 0\t{}\n".format(user, self._user_models[user]))
#         for i in item_in_test:
#             out_file.write("{} 0\t{}\n".format(item, self._item_models[item]))
#
#         out_file.close()
#         in_file.close()


class EvaluateTask(OkapiTrainModelTask):
    '''
    Evaluates how good is the built model.
    We use the trick of loading vertex input and edge input formats.
    In this way, we don't need to join them beforehand.
    '''

    def requires(self):
        return [PrepareMovielensData(), OkapiTrainModelTask(self.model_name, 'movielens.training', self.model_name+"_model")]

    def output(self):
        return luigi.hdfs.HdfsTarget(self.out_hdfs)

    def args(self):
        return [
            "-libjars", ",".join(self.get_libjars()),
            "-Dmapred.child.java.opts="+self._get_conf('hadoop', 'hadoop-mem'),
            "-Dgiraph.zkManagerDirectory="+self._get_conf('hadoop', 'zookeeper-dir'),
            "-Dgiraph.useSuperstepCounters=false",
            self.get_computation_class(),
            '-vif' ,'ml.grafos.okapi.cf.eval.CfModelInputFormat',
            '-vip', self.input()[1],
            '-eif', 'ml.grafos.okapi.cf.eval.CfLongIdBooleanTextInputFormat',
            '-eip', self.input()[0][0],
            '-vof', 'org.apache.giraph.io.formats.IdWithValueTextOutputFormat',
            '-op', self.get_output(),
            '-w', self._get_conf("okapi", "workers")] \
            + self.get_custom_arguments()

    def get_computation_class(self):
        return 'ml.grafos.okapi.cf.eval.RankEvaluationComputation'

    def get_custom_arguments(self):
        return ['-ca', 'minItemId=1',
                '-ca', 'maxItemId=17770',
                '-ca', 'numberSamples='+self._get_conf("okapi", "number-of-negative-samples-in-eval")]

class SpitMePrecision(luigi.Task):
    '''Gives a Precision of the model'''
    model_name = luigi.Parameter(description="The model: {"+" | ".join(methods)+"}")

    def requires(self):
        return EvaluateTask(self.model_name, 'movielens.testing', self.model_name+'_eval')

    def complete(self):
        return False

    def run(self):
        f = self.input().open('r')
        for line in f:
            print line
        f.close()




if __name__ == '__main__':
       luigi.run()