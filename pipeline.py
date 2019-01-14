import logging; logging.basicConfig(level=logging.INFO)
import subprocess

logger = logging.getLogger(__name__)
news_sites_uids = ['eluniversal', 'elpais']

def main():
    _extract()
    _transform()
    _load()


def _extract():
    logging.info('Starting extract process')

    for new_site_uid in news_sites_uids:
        subprocess.run(['python3', 'main.py', new_site_uid], cwd='./Extract')
        # [c]urrent [w]orking [d]irectory
        subprocess.run(['find', '.', '-name', '{}*'.format(new_site_uid),
                       '-exec', 'mv', '{}', '../Transform/{}_.csv'.format(new_site_uid),
                       ';'], cwd='./Extract')


def _transform():
    logging.info('Starting Transform process')

    for new_site_uid in news_sites_uids:
        dirty_data_filename = '{}_.csv'.format(new_site_uid)
        clean_data_filename = 'clean_{}'.format(dirty_data_filename)
        subprocess.run(['python3', 'main.py', new_site_uid], cwd='./Transform')
        subprocess.run(['rm', dirty_data_filename], cwd='./Transform')
        subprocess.run(['mv', clean_data_filename, '../Load/{}.csv'.format(new_site_uid)],
                       cwd='./Transform')


def _load():
    logging.info('Starting load process')

    for new_site_uid in news_sites_uids:
        clean_data_filename = '{}.csv'.format(new_site_uid)
        subprocess.run(['python3', 'main.py', clean_data_filename], cwd='./Load')
        subprocess.run(['rm', clean_data_filename], cwd='./Load')


if __name__ == '__main__':
    main()
