import json

class TableWithPartitions:
    def __init__(self, data):
        #data = json.loads(data)

        self.table = data["Table"]
        self.partition_list = data["PartitionList"]
 