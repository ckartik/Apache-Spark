from pyspark import SparkConf, SparkContext
import collections



conf = SparkConf().setMaster("local").setAppName("WordCounter")
sc = SparkContext(conf = conf)


lines = sc.textFile("file:///SparkCourse/notes.txt")
counts = lines.flatMap(lambda x: x.split(' '))
WordOcurrences = counts.countByValue()

sortedOcurrences = collections.OrderedDict(sorted(WordOcurrences.items(), key=lambda x:x[1]))

for k,v in sortedOcurrences.iteritems():
    print "%s %i" % (k,v)
