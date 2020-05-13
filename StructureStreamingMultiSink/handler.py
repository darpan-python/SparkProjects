from StructureStreamingMultiSink.core import Intializer

class handler():
    def __init__(self, topicname, appname):
        self.topicname = topicname
        self.appname = appname
        self.stream = ""
        pass

    def sparkrun(self):
        it = Intializer(self.topicname, self.appname)
        self.stream = it.startp()
        it.flow(self.stream)
        pass

    pass
h = handler('payments','SparkKafkaConsumer12')
h.sparkrun()