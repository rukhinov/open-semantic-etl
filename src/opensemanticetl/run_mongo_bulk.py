from opensemanticetl.tasks import index_mongo


def import_mongo(verbose=None, start_date=None, end_date=None, self=None):
    index_mongo.apply_async(kwargs={'start_date': str(start_date), 'end_date': str(end_date), 'verbose': str(verbose)},
                            queue='open_semantic_etl_tasks', priority=5)
    print(start_date, end_date)


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

    import_mongo(start_date=options.start_date, end_date=options.end_date, verbose=options.verbose)