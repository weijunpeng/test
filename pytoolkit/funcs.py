from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.serializers import UTF8Deserializer
from pyspark.serializers import NoOpSerializer

from provider import encodeUTF8
from provider import TubeSenderConfig


def loadRDDFromTable(sc, url, use_unicode=True):
    jrdd = sc._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.loadTable(sc._jsc, url)
    return RDD(jrdd, sc, UTF8Deserializer(use_unicode)).map(lambda x: x.split("\01", -1))


def saveRDDToTable(rdd, url, overwrite=True):
    encoded = rdd.map(lambda x: "\01".join(x)).mapPartitions(encodeUTF8)
    encoded._bypass_serializer = True
    rdd.context._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.\
        saveToTable(encoded._jrdd.map(rdd.context._jvm.BytesToString()), url, overwrite)


def loadDataFrameFromTable(session, url):
    df = session._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.loadTable(session._jsparkSession, url)
    return DataFrame(df, session._wrapped)


def saveDataFrameToTable(df, url, overwrite=True):
    df.sql_ctx._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.\
        saveToTable(df._jdf, url, overwrite)


def loadProtobufTable(sc, url):
    jrdd = sc._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.loadProtobufTable(sc._jsc, url)
    return RDD(jrdd, sc, NoOpSerializer())


def saveToProtobufTable(rdd, url, overwrite=True):
    rdd._bypass_serializer = True
    rdd.context._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.\
        saveToProtobufTable(rdd._jrdd, url, overwrite)


def saveStringRDDToTDBank(rdd, config):
    assert isinstance(config, TubeSenderConfig), \
        'config must be instantiated from TDBankTubeSenderConfig'
    jconfig = rdd.context._jvm.com.tencent.tdw.spark.toolkit.tdbank.TubeSenderConfig()\
        .buildFrom(config.bid, config.tid, config.timeout, config.exitOnException)
    encoded = rdd.mapPartitions(encodeUTF8)
    encoded._bypass_serializer = True
    rdd.context._jvm.com.tencent.tdw.spark.toolkit.api.python\
        .PythonTDBankFunctions.saveStringRDD(encoded._jrdd.map(rdd.context._jvm.BytesToString()), jconfig)


def saveArrayByteRDDToTDBank(rdd, config):
    assert isinstance(config, TubeSenderConfig), \
        'config must be instantiated from TDBankTubeSenderConfig'
    jconfig = rdd.context._jvm.com.tencent.tdw.spark.toolkit.tdbank.TubeSenderConfig() \
        .buildFrom(config.bid, config.tid, config.timeout, config.exitOnException)
    rdd.context._jvm.com.tencent.tdw.spark.toolkit.api.python \
        .PythonTDBankFunctions.saveArrayByteRDD(rdd._jrdd, jconfig)


def saveTextStreamToTDBank(dstream, config):
    assert isinstance(config, TubeSenderConfig), \
        'config must be instantiated from TDBankTubeSenderConfig'
    jconfig = dstream.context._jvm.com.tencent.tdw.spark.toolkit.tdbank.TubeSenderConfig() \
        .buildFrom(config.bid, config.tid, config.timeout, config.exitOnException)

    def transformFunc(rdd):
        encoded = rdd.mapPartitions(encodeUTF8)
        encoded._bypass_serializer = True
        return encoded

    encoded = dstream.transform(transformFunc)
    dstream.context._jvm.com.tencent.tdw.spark.toolkit.api.python \
        .PythonTDBankFunctions.saveTextStreamToTDBank(encoded._jdstream.map(dstream.context._jvm.BytesToString()), jconfig)


def saveBytesStreamToTDBank(dstream, config):
    assert isinstance(config, TubeSenderConfig), \
        'config must be instantiated from TDBankTubeSenderConfig'
    jconfig = dstream.context._jvm.com.tencent.tdw.spark.toolkit.tdbank.TubeSenderConfig() \
        .buildFrom(config.bid, config.tid, config.timeout, config.exitOnException)
    dstream.context._jvm.com.tencent.tdw.spark.toolkit.api.python \
        .PythonTDBankFunctions.saveBytesStreamToTDBank(dstream._jdstream, jconfig)