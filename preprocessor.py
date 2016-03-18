"""
Reads from a MongoDB database and writes cleaned input to a new collection
called 'filtered_collection'.
Performs the pre-processing step for topic modeling.
"""
# from BeautifulSoup import BeautifulSoup
from pymongo import MongoClient
from urlparse import urlparse
import justext
import lxml
import nltk


class MongoDBLoader:
    def __init__(self):
        """Set up connection."""
        print("Setting up connection...")
        settings = {'MONGODB_SERVER': "localhost",
                    'MONGODB_PORT': 27017,
                    'MONGODB_DB': "ecosystem_mapping",
                    'MONGODB_FILTERED_COLLECTION': "filtered_collection",
                    'MONGODB_HTML_COLLECTION': "html_collection",
                    'MONGODB_LINK_COLLECTION': "link_collection",
                    'MONGODB_TEXT_COLLECTION': "text_collection"}
        connection = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        self.db = connection[settings['MONGODB_DB']]
        self.filtered_collection = self.db[settings['MONGODB_FILTERED_COLLECTION']]
        self.html_collection = self.db[settings['MONGODB_HTML_COLLECTION']]
        self.link_collection = self.db[settings['MONGODB_LINK_COLLECTION']]

    def load_save(self):
        """Loads in from MongoDB and saves to MySQL."""
        base_urls = self.html_collection.distinct("base_url")
        
        for base_url in base_urls:
            print("Processing URL: %s" % base_url)
            for data in self.html_collection.find({"base_url": base_url}):

                #source = data['src_url']
                source = data['url']
                text = self.clean(data['body'])
                tier = data['tier']
                time = data['timestamp']

                # Apparently, lxml is faster than BeautifulSoup.
                # soup = BeautifulSoup(data['body'])
                # links = [link for link in [x.get('href') for x in soup.findAll('a')]
                #             if link and urlparse(link).netloc != base_url]

                try:
                    dom =  lxml.html.fromstring(data['body'])
                    links = [link for link in dom.xpath('//a/@href')
                        if link and 'http' in link and urlparse(link).netloc != base_url]
                except ValueError:
                    print("ERROR: Did not parse %s" % source)
                    continue

                # print(base_url, links)

                link_item = {
                    "base_url": base_url,
                    "dst_url": links,
                    "src_url": source,
                    "tier": tier,
                    "timestamp": time
                }

                text_item = {
                    "base_url": base_url,
                    "src_url": source,
                    "text": text,
                    "tier": tier,
                    "timestamp": time
                }

                self.filtered_collection.insert_one(text_item)
                self.link_collection.insert_one(link_item)

    def clean(self, text):
        """
        Cleans HTML text by removing boilerplates and filtering unnecessary
        words, e.g. geographical and date/time snippets.
        """

        text = self.remove_boilerplate(text)
        text = self.remove_named_entity(text)
        return text
    
    def remove_named_entity(self, text):
        _text = list()
        for idx, sent in enumerate(nltk.sent_tokenize(text)):
            for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent)), binary = True):
                #if hasattr(chunk, 'lab'):
                if type(chunk) is not nltk.Tree:
                    word, pos = chunk
                    # if pos == " "  for further removal
                    _text.append(word)
                else:
                    #ne = ' '.join(c[0] for c in chunk.leaves())
                    #self.named_entities.append(ne)
                    continue
        return ' '.join(_text)

    def remove_boilerplate(self, text):
        jtext = justext.justext(text, justext.get_stoplist("English"))
        cleaned = [line.text for line in jtext if not line.is_boilerplate]
        cleaned_text = " ".join(cleaned) if cleaned else ""
        return cleaned_text

    def filter_document(self, text):
        # Deprecated

        # filtered = []
        # custom_stopwords = ['div']
        # for word in document.split():
        #     if not (word.isdigit() or word[0] == '-' and word[1:].isdigit()) and (word.lower() not in custom_stopwords):
        #         filtered.append(word)
        # return " ".join(filtered)

        #return text
        pass

if __name__ == "__main__":
    MongoDBLoader().load_save()
