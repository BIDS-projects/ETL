"""
Preprocessor for ETL

Reads from the MongoDB databse from the scraper, cleans the input, and writes
to several MySQL databases for different analysis routes.

Usage:
    preprocessor.py [-a] [-l] [-r] [-t]

Options:
    -h --help           Display usage instructions.
    -a --all            Runs all preprocessing steps.
    -l --link           Runs the link processor for visualization.
    -r --researchers    Runs the researcher extraction.
    -t --text           Runs the text processor for topic modeling.
"""
from db import LinkItem, MySQL, MySQLConfig, FromItem, ResearcherItem, ToItem
from docopt import docopt
from pymongo import MongoClient
from sqlalchemy.orm import relationship
from urlparse import urlparse
import codecs
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

        if self.options['--all'] or self.options['--link'] or self.options['--researchers']:
            print("Setting up MySQL connection...")
            self.mySQL = MySQL(config=MySQLConfig)

        # Initialize the perceptron for fast tagging
        if self.options['--all'] or self.options['--researchers'] or self.options['--text']:
            self.tagger = nltk.tag.perceptron.PerceptronTagger()

        # Initialize the pickle
        if self.options['--all'] or self.options['--researchers']:
            self.chunker = nltk.data.load('chunkers/maxent_ne_chunker/english_ace_multiclass.pickle')
        elif self.options['--text']:
            self.chunker = nltk.data.load('chunkers/maxent_ne_chunker/english_ace_binary.pickle')

        import sys

        reload(sys)
        sys.setdefaultencoding('utf8')

    def process(self):
        """
        Extract raw data from MongoDB and save transformed data to MySQL.
        """
        base_urls = self.html_collection.distinct("base_url")
        
        for base_url in base_urls:
            print("Processing URL: %s" % base_url)
            for data in self.html_collection.find({"base_url": base_url}):
                self.transform_and_load(data)

    def transform_and_load(self, data):
        """ Transform the raw data into useful formats and load them into MySQL database"""
        base_url = data['base_url']
        src_url = data['url']
        tier = data['tier']
        timestamp = data['timestamp']

        try:
            dom =  lxml.html.fromstring(data['body'])
            links = [link for link in dom.xpath('//a/@href')
                if link and 'http' in link and urlparse(link).netloc != base_url]
        except ValueError:
            print("ERROR: Did not parse %s." % src_url)
            return

        if self.options['--all'] or self.options['--link']:
            from_item = FromItem(base_url=bytes(base_url))
            for link in links:
                link = urlparse(link).netloc
                from_item.to_items.append(ToItem(base_url=link))
            from_item.save()

        if self.options['--all'] or self.options['--researchers'] or self.options['--text']:
            text, researchers = self.clean(data['body'])

        if self.options['--all'] or self.options['--text']:
            self.filtered_collection.insert_one({
                "base_url": base_url,
                "src_url": src_url,
                "text": text,
                "tier": tier,
                "timestamp": timestamp
            })

        if self.options['--all'] or self.options['--researchers']:
            link = LinkItem(base_url=bytes(base_url))
            for researcher in researchers:
                link.researchers.append(ResearcherItem(name=bytes(researcher), domain=bytes(base_url)))
            if link.researchers:
                link.save()


    def clean(self, text):
        """
        Cleans HTML text by removing boilerplates and filtering unnecessary
        words, e.g. geographical and date/time snippets.
        """
        _text, researchers = [], []
        text = self.remove_boilerplate(text)

        tok_sents = [nltk.word_tokenize(sent) for sent in nltk.sent_tokenize(text)]
        pos_sents = [nltk.tag._pos_tag(sent, None, self.tagger) for sent in tok_sents]
        chunked_sents = self.chunker.parse_sents(pos_sents)
        for sent in chunked_sents:
            for chunk in sent:
                if self.options['--all'] or self.options['--text']:
                    word = self.remove_named_entity(chunk)
                    if word:
                        _text.append(word)
                if self.options['--all'] or self.options['--researchers']:
                    resarcher = self.extract_researcher(chunk)
                    if researcher:
                        researchers.append(researcher)

        text = self.remove_stop_words(_text)
        return text, researchers

    def remove_boilerplate(self, text):
        """
        Removes website artifacts: "Skip to Main Content", "About Us", etc.
        """
        jtext = justext.justext(text, justext.get_stoplist("English"))
        cleaned = [line.text for line in jtext if not line.is_boilerplate]
        cleaned_text = " ".join(cleaned) if cleaned else ""
        return cleaned_text

    def remove_named_entity(self, chunk):
        """
        Removes proper nouns (e.g. geographical locations).
        """
        if type(chunk) is not nltk.Tree:
            word, pos = chunk
            return word

    def remove_stop_words(self, word_list):
        filtered_words = [word for word in word_list if word not in nltk.corpus.stopwords.words('english')]
        return ' '.join(filtered_words)

    def extract_researcher(self, chunk):
        if hasattr(chunk, 'label') and chunk.label:
            if chunk.label() == 'PERSON':
                return ' '.join([child[0] for child in sent])



if __name__ == "__main__":
    arguments = docopt(__doc__)
    MongoDBLoader(arguments).process()
