#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
United Nations Economic Commission for Africa
HDX data scraper
"""

from bs4 import BeautifulSoup
import requests
import csv
import yaml


def getHTML(id):
    """
    Get the raw html from the ECA
    """
    url = "https://ecastats.uneca.org/data/BrowseData.aspx?Id=%d" % id
    response = requests.get(url)
    return BeautifulSoup(response.text, 'lxml')


def getCountryName(soupe):
    """
    Get the current page index's country name
    """
    h3 = soupe.findAll('h3')
    for h in h3:
        ch = h.find('b').get_text().strip()
        if len(ch) == 0:
            pass
        else:
            return ch.upper()


def getResources(countryName, dataResource, soup):
    """
    Generate indicators data for a country
    """
    for dr in dataResource:
        table = soup.find('table', id=dataResource[dr]['dTableID'])
        rows = table.findAll('tr')

        nbRows = 0
        with open('data/%s-' % countryName + '%s.csv' % dr, 'w') as file:
            writer = csv.writer(file, delimiter=',')
            for r in rows:
                currentCell = r.findAll('td')
                data = []
                for c in currentCell:
                    data.append(c.get_text())

                if nbRows == 0:
                    writer.writerow(data)
                    nbRows += 1
                    # add hxl tags
                else:
                    writer.writerow(data)
        # end with
        tableDS = soup.find('table', id=dataResource[dr]['sTableID'])
        dsRows = tableDS.findAll('tr')
        nbRows = 0
        source = []
        for r in dsRows:
            currentCell = r.findAll('td')
            for c in currentCell:
                if nbRows == 0:
                    nbRows += 1
                else:
                    source.append(c.get_text())
        # write sources to the metadata yaml file
        metadata = yaml.load(open('config/metadata.yml', 'r'))
        if dr == 'population_and_migration':
            metadata['population_and_migration']['data_source'] = ','.join(source)
        else:
            if dr == 'health':
                metadata['health']['data_source'] = ','.join(source)
            else:
                if dr == 'education':
                    metadata['education']['data_source'] = ','.join(source)
        yaml.dump(metadata, open('config/metadata.yml', 'w'),
                  default_flow_style=False)

def completMetadata():
    """
    Add datasets names and titles in config/metadata.yml file
    """



def generateDatasets(pageID):
    # get the html content once in a global variable
    soup = getHTML(pageID)
    countryName = getCountryName(soup)

    dataResource = {'population_and_migration': {'dTableID': 'StatBasebody_tblPopulation', 'sTableID': 'StatBasebody_tblPopulationDS'},
                    'health': {'dTableID': 'StatBasebody_tblHealth', 'sTableID': 'StatBasebody_tblHealthDS'},
                    'education': {'dTableID': 'StatBasebody_tblEducation', 'sTableID': 'StatBasebody_tblEducationDS'}
                    }
    getResources(countryName, dataResource, soup)

