"""
Reads from a MongoDB database and writes cleaned input to a new collection
called 'filtered_collection'.
Performs the pre-processing step for topic modeling.
"""
from pymongo import MongoClient
import justext


class MongoDBLoader:
    def __init__(self):
        """Set up connection."""
        print("Setting up connection...")
        settings = {'MONGODB_SERVER': "localhost",
                    'MONGODB_PORT': 27017,
                    'MONGODB_DB': "ecosystem_mapping",
                    'MONGODB_FILTERED_COLLECTION': "filtered_collection",
                    'MONGODB_HTML_COLLECTION': "html_collection",
                    'MONGODB_TEXT_COLLECTION': "text_collection"}
        connection = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        self.db = connection[settings['MONGODB_DB']]
        self.html_collection = self.db[settings['MONGODB_HTML_COLLECTION']]
        self.filtered_collection = self.db[settings['MONGODB_FILTERED_COLLECTION']]

    def load_save(self):
        """Loads in from MongoDB and saves to MySQL."""
        base_urls = self.html_collection.distinct("base_url")
        for base_url in base_urls:
            print("Cleaning input from URL: %s" % base_url)
            tier = float('inf')
            for data in self.html_collection.find({"base_url": base_url}):
                text = self.clean(data['body'])
                tier = min(tier, data['tier'])
                text_item = {
                    "base_url": base_url,
                    "text": text,
                    "tier": tier
                }
                self.filtered_collection.insert_one(text_item)

    def clean(self, text):
        """
        Cleans HTML text by removing boilerplates and filtering unnecessary
        words, e.g. geographical and date/time snippets.
        """
        jtext = justext.justext(text, justext.get_stoplist("English"))
        cleaned = [line.text for line in jtext if not line.is_boilerplate]
        cleaned_text = " ".join(cleaned) if cleaned else ""

        # TODO: Insert more filters.

        return self.filter_document(cleaned_text)

    def filter_document(self, text):
        # TODO: Add filtering

        # filtered = []
        # custom_stopwords = ['div']
        # for word in document.split():
        #     if not (word.isdigit() or word[0] == '-' and word[1:].isdigit()) and (word.lower() not in custom_stopwords):
        #         filtered.append(word)
        # return " ".join(filtered)

        return text


MongoDBLoader().load_save()
