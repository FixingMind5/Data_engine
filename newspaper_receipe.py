import argparse
import logging
from urllib.parse import urlparse
import pandas as pd

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

def main(filename):
    """Path of the main function"""
    logger.info('Sarting cleaning process')
    df = _read_data(filename)
    newspaper_uid = _extract_newspaper_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)

    return df


def _read_data(filename):
    """Reads the file"""
    logger.info('Reading file {}'.format(filename))

    return pd.read_csv(filename)


def _extract_host(df):
    """Exctract url direction"""
    logger.info('Extracting host from urls')

    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)

    return df


def _add_newspaper_uid_column(df, newspaper_uid):
    """Adds the newspaper uid to a column"""
    logger.info('Filling newspaper column with {}'.format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid

    return df


def _extract_newspaper_uid(filename):
    """Extracts newspaper uid from the filename"""
    logger.info('Stracting newspaper uid...')
    newspaper_uid = filename.split('_')[0]

    logger.info('Newspaper uid has been detected {}'.format(newspaper_uid))

    return newspaper_uid


def _fill_missing_titles(df):
    """Fills the NaN data"""
    logger.info('Filling missing titles')

    missing_titles_mask = df['title'].isna()
    missing_titles = (df[missing_titles_mask]['url']
                       .str.extract(r'(?P<missing_titles>[^/]+)$')
                       .applymap(lambda title: title.split('-'))
                       .applymap(lambda title_word_list: ' '.join(title_word_list))
                     )
    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']

    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('filename',
                    help='the path to the dirty data',
                    type=str)

    args = parser.parse_args()
    df = main(args.filename)
    print(df)
