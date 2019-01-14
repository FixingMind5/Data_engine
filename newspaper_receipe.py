import argparse
import logging
import pandas as pd
import hashlib
import nltk

from nltk.corpus import stopwords
from urllib.parse import urlparse

stopwords = set(stopwords.words('spanish'))
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
    df = _generate_uid_for_rows(df)
    df = _remove_new_lines_from_body(df)
    column_name = input('Name of column is...\t')
    df['n_tokenize_{}'.format(column_name)] = _tokenizing_title_(df, column_name)
    return df


def _tokenizing_title_(df, column_name):
    logger.info('Ready for tokenize {}'.format(column_name))

    return (df
                .dropna()
                .apply(lambda row: nltk.word_tokenize(row[column_name]), axis = 1)
                .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
                .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
                .apply(lambda valid_word_list: len(valid_word_list))
           )


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


def _generate_uid_for_rows(df):
    """Generating uids for each row"""
    logger.info('generating uids for each row')

    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
           )

    df['uid'] = uids
    return df.set_index('uid')


def _remove_new_lines_from_body(df):
    logger.info('Removing new lines from body')

    stripped_body = (df
                    .apply(lambda row: row['body'], axis=1)
                    .apply(lambda body: list(body))
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\n', ''), letters)))
                    .apply(lambda letters: ''.join(letters))
                   )
    df['body'] = stripped_body

    return df


if __name__ == '__main__':

    if nltk.tokenize.punkt:
        pass
    elif nltk.stopwords:
        pass
    else:
        nltk.download('punkt')
        nltk.download('stopwords')

    parser = argparse.ArgumentParser()

    parser.add_argument('filename',
                    help='the path to the dirty data',
                    type=str)

    args = parser.parse_args()
    df = main(args.filename)
    print(df)
