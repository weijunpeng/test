from pyspark import SparkContext


class TableDesc(object):
    """
    for create table
    """
    def __init__(self):
        self._tblName = None
        self._cols = None
        self._comment = None
        self._fileFormat = None
        self._compress = False
        self._partType = None
        self._partField = None
        self._subPartType = None
        self._subPartField = None
        self._fieldDelimiter = None

    @property
    def tblName(self):
        return self._tblName

    def setTblName(self, tblName):
        self._tblName = tblName
        return self

    @property
    def cols(self):
        return self._cols

    def setCols(self, cols):
        self._cols = cols
        return self

    @property
    def comment(self):
        return self._comment

    def setComment(self, comment):
        self._comment = comment
        return self

    @property
    def fileFormat(self):
        return self._fileFormat

    def setFileFormat(self, fileFormat):
        self._fileFormat = fileFormat
        return self

    @property
    def compress(self):
        return self._compress

    def setCompress(self, compress):
        self._compress = compress
        return self

    @property
    def partType(self):
        return self._partType

    def setPartType(self, partType):
        self._partType = partType
        return self

    @property
    def partField(self):
        return self._partField

    def setPartField(self, partField):
        self._partField = partField
        return self

    @property
    def subPartType(self):
        return self._subPartType

    def setSubPartType(self, subPartType):
        self._subPartType = subPartType
        return self

    @property
    def subPartField(self):
        return self._subPartField

    def setSubPartField(self, subPartField):
        self._subPartField = subPartField
        return self

    @property
    def fieldDelimiter(self):
        return self._fieldDelimiter

    def setFieldDelimiter(self, fieldDelimiter):
        self._fieldDelimiter = fieldDelimiter
        return self


class TableInfo(object):
    def __init__(self, s_table_info):
        """
        build from scala table info
        :param s_table_info:
        :return:
        """
        self._s_table_info = s_table_info

    @property
    def dbName(self):
        return self._s_table_info.dbName()

    @property
    def tblName(self):
        return self._s_table_info.tblName()

    @property
    def inputFormat(self):
        return self._s_table_info.inputFormat()

    @property
    def outputFormat(self):
        return self._s_table_info.outputFormat()

    @property
    def colNames(self):
        return self._s_table_info.getColNames()

    @property
    def colTypes(self):
        return self._s_table_info.getColTypes()

    @property
    def colComments(self):
        return self._s_table_info.getColComments()

    @property
    def compressed(self):
        return self._s_table_info.compressed()

    @property
    def priPartField(self):
        return self._s_table_info.priPartField()

    @property
    def priPartType(self):
        return self._s_table_info.priPartType()

    @property
    def subPartField(self):
        return self._s_table_info.subPartField()

    @property
    def subPartType(self):
        return self._s_table_info.subPartType()

    @property
    def partitions(self):
        parts = []
        str_partitions = self._s_table_info.getPartitions()
        for part in str_partitions:
            tmp = part.split(":")
            assert len(tmp) == 3
            if tmp[2] == "":
                parts.append(Partition(int(tmp[0]), tmp[1], []))
            else:
                parts.append(Partition(int(tmp[0]), tmp[1], tmp[2].split(",")))
        return parts

    @property
    def fieldDelimiter(self):
        return self._s_table_info.fieldDelimiter()


class Partition(object):
    def __init__(self, level, name, values):
        self.level = level
        self.name = name
        self.values = values

    def __repr__(self):
        return "%d:%s:%s" % (self.level, self.name, ",".join(self.values))

    def __str__(self):
        return "%d:%s:%s" % (self.level, self.name, ",".join(self.values))


class TDWUtil(object):
    def __init__(self, user=None, passwd=None, dbName=None, group="tl"):
        # To support tdw-spark-toolkit 3.6.0, the argument dbName's default value
        # is set None, but it can not be None
        assert dbName is not None, 'dbName can not be None'
        self._jvm = SparkContext._jvm
        self._s_tdw_util = self._jvm.\
            com.tencent.tdw.spark.toolkit.tdw.TDWUtil(user, passwd, dbName, group)

    def createTable(self, tblDesc):
        s_table_desc = self._jvm.\
            com.tencent.tdw.spark.toolkit.tdw.TableDesc().buildFrom(tblDesc.tblName,
                                                                    tblDesc.cols,
                                                                    tblDesc.comment,
                                                                    tblDesc.fileFormat,
                                                                    tblDesc.compress,
                                                                    tblDesc.partType,
                                                                    tblDesc.partField,
                                                                    tblDesc.subPartType,
                                                                    tblDesc.subPartField,
                                                                    tblDesc.fieldDelimiter)
        self._s_tdw_util.createTable(s_table_desc)

    def tableExist(self, tblName):
        return self._s_tdw_util.tableExist(tblName)

    def partitionExist(self, tblName, partName):
        return self._s_tdw_util.partitionExist(tblName, partName)

    def dropTable(self, tblName):
        self._s_tdw_util.dropTable(tblName)

    def truncateTable(self, tblName):
        self._s_tdw_util.truncateTable(tblName)

    def createRangePartition(self, tblName, partName, upperBound, level=0):
        self._s_tdw_util.createRangePartition(tblName, partName, upperBound, level)

    def createListPartition(self, tblName, partName, listValue, level=0):
        self._s_tdw_util.createListPartition(tblName, partName, listValue, level)

    def createDefaultPartition(self, tblName, level=0):
        self._s_tdw_util.createDefaultPartition(tblName, level)

    def dropPartition(self, tblName, partName, level=0):
        self._s_tdw_util.dropPartition(tblName, partName, level)

    def truncatePartition(self, tblName, partName, subPartName=None):
        self._s_tdw_util.truncatePartition(tblName, partName, subPartName)

    def getTableInfo(self, tblName):
        s_table_info = self._s_tdw_util.getTableInfo(tblName)
        return TableInfo(s_table_info)

    def showRowCount(self, tblName, priParts=[], subParts=[]):
        return self._s_tdw_util.pythonShowRowCount(tblName, "\01".join(priParts), "\01".join(subParts))

    def tableSize(self, tblName, priParts=[], subParts=[]):
        return self._s_tdw_util.pythonTableSize(tblName, "\01".join(priParts), "\01".join(subParts))
