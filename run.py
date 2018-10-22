from eca import generateDatasets

import logging


from hdx.hdx_configuration import Configuration
from os.path import join

from hdx.facades.simple import facade


logger = logging.getLogger(__name__)


def main():
    datasets = generateDatasets(32)
    for dataset in datasets:
    	dataset.update_from_yaml()
    	dataset.check_required_fields(ignore_fields=['notes'])

    	dataset.create_in_hdx()


if __name__ == '__main__':
    facade(main, hdx_site='test', user_agent='HDXINTERNAL UNECA scraper')
