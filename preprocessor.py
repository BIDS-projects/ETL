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
from db import DomainItem, LinkItem, MySQL, MySQLConfig, FromItem, ResearcherItem, ToItem
from docopt import docopt
from fuzzywuzzy import process
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

        faculty = codecs.open('researchers.csv', 'r', encoding='utf-8')
        faculty = faculty.read()
        self.faculty = []
        for member in faculty.splitlines():
            self.faculty.append(member)
        self.tolerance = 85

        import sys

        reload(sys)
        sys.setdefaultencoding('utf8')

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
                    from_item = FromItem(base_url=bytes(base_url))
                    for link in links:
                        link = urlparse(link).netloc
                        from_item.to_items.append(ToItem(base_url=link))
                    from_item.save()

                text = self.clean(base_url, data['body'])
                if self.options['--all'] or self.options['--text']:
                    self.filtered_collection.insert_one({
                        "base_url": base_url,
                        "src_url": src_url,
                        "text": text,
                        "tier": tier,
                        "timestamp": timestamp
                    })

    def clean(self, base_url, text):
        """
        Cleans HTML text by removing boilerplates and filtering unnecessary
        words, e.g. geographical and date/time snippets.
        """
        text = self.remove_boilerplate(text)
        text = self.remove_named_entity(text)

        if self.options['--all'] or self.options['--researchers']:
            domain = DomainItem(domain=bytes(base_url))
            researchers = self.extract_researchers(text)
            for researcher in researchers:
                domain.researchers.append(ResearcherItem(name=bytes(researcher)))
            if domain.researchers:
                domain.save()

        return text

    def remove_named_entity(self, text):
        """
        Removes proper nouns (e.g. geographical locations).
        """
        _text = list()

        tok_sents = [nltk.word_tokenize(sent) for sent in nltk.sent_tokenize(text)]
        pos_sents = nltk.pos_tag_sents(tok_sents)
        chunked_sents = nltk.ne_chunk_sents(pos_sents, binary = True)

        for sent in chunked_sents:
            for chunk in sent:
                if type(chunk) is not nltk.Tree:
                    word, pos = chunk
                    # if pos == " ":  for further removal
                    _text.append(word)
                else:
                    continue
        return ' '.join(_text)

    def extract_researchers(self, text):
        researchers = []
        sentences = nltk.tokenize.sent_tokenize(text)
        tokenized_sentences = [nltk.word_tokenize(sentence) for sentence in sentences]
        tagged_sentences = nltk.pos_tag_sents(tokenized_sentences)
        chunks = nltk.ne_chunk_sents(tagged_sentences, binary=False)
        for chunk in chunks:
            for sent in chunk:
                if hasattr(sent, 'label') and sent.label:
                    if sent.label() == 'PERSON':
                        researchers.append(' '.join([child[0] for child in sent]))
        researchers = map(lambda researcher: process.extractOne(researcher, self.faculty), researchers)
        researchers = [researcher[0] for researcher in researchers if researcher and researcher[1] >= self.tolerance]
        return researchers

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
