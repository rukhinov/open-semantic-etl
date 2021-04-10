import os
import sys
import pymongo
from datetime import datetime, timedelta
from etl import ETL
from dateparser import parse as date_parse
from dotenv import load_dotenv

load_dotenv()

etl = ETL()
etl.config['plugins'] = ['enhance_ner_spacy', 'enhance_extract_hashtags', 'clean_title', 'enhance_path',
                         'enhance_entity_linking', 'enhance_multilingual']


def index(verbose=None, start_date=None, end_date=None, self=None):
    global id, parameters, data
    mongo_uri = os.getenv('MONGO_URI')
    #    print(mongo_uri)
    client = pymongo.MongoClient(mongo_uri)

    # Database Name
    database = os.getenv('MONGO_DATABASE')
    db = client[database]

    # Collection Name
    col = db["articles"]

    if end_date is None:
        end_date = datetime.now()  # end_date = datetime.utcnow()
    else:
        end_date = date_parse(end_date)

    if verbose:
        print(
            "end_date is {}".format(end_date))

    if start_date is None:
        start_date = end_date - timedelta(hours=1)  # start_date = datetime.utcnow() - timedelta(hours=1)
    else:
        start_date = date_parse(start_date)

    if verbose:
        print(
            "start_date is {}".format(start_date))

    x = col.find({"last_crawled": {"$lt": end_date, "$gte": start_date}})

    for article in x:
        try:
            id = article['metadata']['webUrl']
            if verbose:
                print(
                    "Article {} is being indexed".format(id))
            parameters = {'id': 'article'}
            data = {}
            data['id'] = article['metadata']['webUrl']
            data['content_type_ss'] = 'Article'
            data['content_type_group_ss'] = 'News Article'
            data['title_txt'] = article['metadata']['webTitle']
            data['content_txt'] = article['metadata']['fields']['bodyText']
            data['description_txt'] = article['metadata']['fields']['trailText']
            data['author_ss'] = article['metadata']['fields']['byline']
            data['pubdate_ss'] = article['metadata']['webPublicationDate']
        except KeyError as ke:
            print('Key Not Found in Article Dictionary:', ke)

        try:
            etl.process(parameters, data)
            # if exception because user interrupted by keyboard, respect this and abort
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except BaseException as e:
            sys.stderr.write(
                "Exception adding for id {} : {}".format(id, e))

            if 'raise_pluginexception' in self.config:
                if self.config['raise_pluginexception']:
                    raise e


if __name__ == "__main__":
    from optparse import OptionParser

    # get start_date and end_date for query

    parser = OptionParser("etl-mongo [options]")
    parser.add_option("-e", "--end_date", dest="end_date",
                      default=None, help="End_date, e.g. --end_date='3 days ago at 14 pm'")
    parser.add_option("-s", "--start_date", dest="start_date",
                      default=None, help="Start_date, e.g. --start_date='1 days ago at 14 pm'")
    parser.add_option("-v", "--verbose", dest="verbose",
                      action="store_true", default=None, help="Print debug messages")

    (options, args) = parser.parse_args()

    index(start_date=options.start_date, end_date=options.end_date, verbose=options.verbose)