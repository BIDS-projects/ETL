"Loads MongoDB to MySQL for queries"


from db import MySQL, MySQLConfig
from db import MySQLObject
from pymongo import MongoClient

class MongoDBLoader:
    def __init__(self):
        """Set up connection."""
        print("Setting up connection...")
        settings = {'MONGODB_SERVER': "localhost",
                    'MONGODB_PORT': 27017,
                    'MONGODB_DB': "ecosystem_mapping",
                    'MONGODB_FILTERED_COLLECTION': "filtered_collection",
                    'MONGODB_LINK_COLLECTION': "link_collection"}
        connection = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        self.db = connection[settings['MONGODB_DB']]
        self.link_collection = self.db[settings['MONGODB_LINK_COLLECTION']]
        self.filtered_collection = self.db[settings['MONGODB_FILTERED_COLLECTION']]

    def load_save(self):
        """Loads in from MongoDB and saves to MySQL."""
        mysql = MySQL(config=MySQLConfig)
        urls = list()
        base_urls = self.link_collection.distinct("base_url")
        for base_url in base_urls:
            for data in self.link_collection.find({"base_url": base_url}):
                src_url = data['src_url']
                dst_url = data['dst_url']
                urls.append(data)
                MySQLObject(base_url=bytes(base_url, 'utf-8'), 
                    src_url=bytes(src_url, 'utf-8'), 
                    links = str(dst_url)).save()
                print("adding %s to MySQL" % base_url)

MongoDBLoader().load_save()
    























