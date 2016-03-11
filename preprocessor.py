"""
Preprocessor for ETL

Reads from the MongoDB databse from the scraper, cleans the input, and writes
to several MySQL databases for different analysis routes.

Usage:
    preprocessor.py [-a] [-l] [-t]

Options:
    -h --help  Display usage instructions.
    -a --all   Runs all preprocessing steps.
    -l --link  Runs the link processor for visualization.
    -t --text  Runs the text processor for topic modeling.
"""
from db import LinkItem, MySQL, MySQLConfig, FromItem, ToItem
from docopt import docopt
from pymongo import MongoClient
from sqlalchemy.orm import relationship
from urlparse import urlparse
import justext
import lxml
import nltk


class MongoDBLoader:

    def __init__(self, options):
        """
        Sets up connections to MongoDB and MySQL.
        """
        self.options = options

        print("Setting up MongoDB connection...")
        settings = {'MONGODB_SERVER': "localhost",
                    'MONGODB_PORT': 27017,
                    'MONGODB_DB': "ecosystem_mapping",
                    'MONGODB_FILTERED_COLLECTION': "filtered_collection",
                    'MONGODB_HTML_COLLECTION': "html_collection"}
        connection = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        self.db = connection[settings['MONGODB_DB']]
        self.filtered_collection = self.db[settings['MONGODB_FILTERED_COLLECTION']]
        self.html_collection = self.db[settings['MONGODB_HTML_COLLECTION']]

        print("Setting up MySQL connection...")
        self.mySQL = MySQL(config=MySQLConfig)

    def load_save(self):
        """
        Loads in from MongoDB and saves to MySQL.
        """
        base_urls = self.html_collection.distinct("base_url")
        
        for base_url in base_urls:
            print("Processing URL: %s" % base_url)
            for data in self.html_collection.find({"base_url": base_url}):

                src_url = data['url']
                tier = data['tier']
                timestamp = data['timestamp']

                try:
                    dom =  lxml.html.fromstring(data['body'])
                    links = [link for link in dom.xpath('//a/@href')
                        if link and 'http' in link and urlparse(link).netloc != base_url]
                except ValueError:
                    print("ERROR: Did not parse %s." % src_url)
                    continue

                if self.options['--all'] or self.options['--link']:
                    # import pdb; pdb.set_trace()
                    from_item = FromItem(base_url=bytes(base_url))
                    for link in links:
                        link = urlparse(link).netloc
                        from_item.to_items.append(ToItem(base_url=link))
                    from_item.save()

                if self.options['--all'] or self.options['--text']:
                    text = self.clean(data['body'])
                    self.filtered_collection.insert_one({
                        "base_url": base_url,
                        "src_url": src_url,
                        "text": text,
                        "tier": tier,
                        "timestamp": timestamp
                    })

    def clean(self, text):
        """
        Cleans HTML text by removing boilerplates and filtering unnecessary
        words, e.g. geographical and date/time snippets.
        """
        text = self.remove_boilerplate(text)
        text = self.remove_named_entity(text)
        return text
    
    def remove_named_entity(self, text):
        """
        Removes proper nouns (e.g. geographical locations).
        """
        _text = list()
        for idx, sent in enumerate(nltk.sent_tokenize(text)):
            for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent)), binary=True):
                # if hasattr(chunk, 'lab'):
                if type(chunk) is not nltk.Tree:
                    word, pos = chunk
                    # if pos == " ": # for further removal
                    _text.append(word)
                else:
                    # ne = ' '.join(c[0] for c in chunk.leaves())
                    # self.named_entities.append(ne)
                    continue
        return ' '.join(_text)
        # return text

    def remove_boilerplate(self, text):
        """
        Removes website artifacts: "Skip to Main Content", "About Us", etc.
        """
        jtext = justext.justext(text, justext.get_stoplist("English"))
        cleaned = [line.text for line in jtext if not line.is_boilerplate]
        cleaned_text = " ".join(cleaned) if cleaned else ""
        return cleaned_text


if __name__ == "__main__":
    arguments = docopt(__doc__)
    MongoDBLoader(arguments).load_save()
