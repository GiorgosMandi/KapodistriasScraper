import pandas as pd
import requests
import pickle
import re
from bs4 import BeautifulSoup


def wiki_scraper():
    try:
        with open("datasets/Kapodistrias_scheme/Wiki_Object", 'rb') as wo:
            page = pickle.load(wo)
    except FileNotFoundError:
        page = requests.get("https://el.wikipedia.org/wiki/%CE%94%CE%B9%CE%BF%CE%B9%CE%BA%CE%B7%CF%84%CE%B9%CE%BA%CE%AE_%CE%B4%CE%B9%CE%B1%CE%AF%CF%81%CE%B5%CF%83%CE%B7_%CF%84%CE%B7%CF%82_%CE%95%CE%BB%CE%BB%CE%AC%CE%B4%CE%B1%CF%82_1997")
        if page.status_code == 200:
            with open("datasets/Kapodistrias_scheme/Wiki_Object", 'wb') as wo:
                pickle.dump(page, wo)

    region_prefectures = {}
    prefectures_municipalities = {}
    municipalities_districts = {}

    # gets the table from the html page
    soup = BeautifulSoup(page.content, 'html.parser')
    html = list(soup.children)[2]
    main_table = html.find('table')
    sub_tables = main_table.find_all('table')
    data = sub_tables[0].find_all('td')

    # parses the wikipedia pages -- firstly stores the region, then stores the
    # prefecture and request to the wikipedia page to get prefectures' municipalities.
    # The data are stored in dictionaries where the key is the region/prefectures
    # and the values are the prefectures/municipalities that belongs to it.
    # NOTE: special treatment for Attica Region
    r_key = re.sub('\n', '', data[0].get_text())
    r_prefectures = []
    c_key = ""
    for index, prefecture in enumerate(data[2:]):
        if index % 2 == 0:
            c_key = re.sub('\n', '', prefecture.get_text())
            r_prefectures.append(c_key)

        else:
            print('GET ', prefecture.get_text())
            url = prefecture.find('a',  href=True)
            munic_page = requests.get('https://el.wikipedia.org/'+url['href'])
            sub_soup = BeautifulSoup(munic_page.content, 'html.parser')
            sub_html = list(sub_soup.children)[2]
            municipalities_list = sub_html.find_all('ul')[1]
            municipalities_li = municipalities_list.find_all('li')
            municipalities = []
            for municipality in municipalities_li:
                municipality_str = re.sub('\d|-- | --|\.|\n', '', municipality.get_text())
                if municipality_str[0] == ' ':
                    municipality_str = municipality_str[1:]

                # NOTE: in order to skip an error in Peiraios prefecture
                if len(municipality_str.split(' ')) > 1 and \
                        (municipality_str.split(' ')[0] == "Κοινότητα" or municipality_str.split(' ')[0] == "Δήμος"):
                    municipalities.append(municipality_str)

                    # Gets the Regions
                    # constructs the id of the element that contains the districts
                    munic_id = "--_" + municipality_str.replace(" ", "_") + "_--"
                    sub = sub_html.find('span', {"id": munic_id})
                    if sub is None:
                        # Many cases!
                        munic_id = municipality_str.replace(" ", "_")
                        sub = sub_html.find('span', {"id": munic_id})

                    # finds the element that the districts names are located
                    flag = sub is None
                    while not flag and sub.name != 'dl':
                        sub = sub.next_element
                        flag = sub is None

                    districts = []
                    if not flag:
                        # gets the districts and cleans their text
                        for au in sub.find_all('dd'):
                            if au.find('b') is not None:
                                district_str = au.find('b').get_text()

                                # clean str
                                district_str = re.sub('\d|[|]', '', district_str)
                                if district_str[0] == ' ':
                                    district_str = district_str[1:]
                                if municipality_str[-1] == ' ':
                                    district_str = district_str[:-1]

                                districts.append(district_str)

                    # insertion to the output
                    municipalities_districts[municipality_str] = pd.Series(districts)
            prefectures_municipalities[c_key] = pd.Series(municipalities)
    region_prefectures[r_key] = pd.Series(r_prefectures)

    # for the rest of the regions
    for table in sub_tables[1:]:
        data = table.find_all('td')
        r_key = re.sub('\n', '', data[0].get_text())
        r_prefectures = []
        for index, prefecture in enumerate(data[1:]):
            if index % 2 == 0:
                c_key = re.sub('\n', '', prefecture.get_text())
                r_prefectures.append(c_key)

            else:
                # gets municipalities
                print("GET ", prefecture.get_text())
                url = prefecture.find('a',  href=True)
                munic_page = requests.get('https://el.wikipedia.org/'+url['href'])
                munic_soup = BeautifulSoup(munic_page.content, 'html.parser')
                sub_html = list(munic_soup.children)[2]
                municipalities_list = sub_html.find_all('ul')[1]
                municipalities_li = municipalities_list.find_all('li')
                municipalities = []
                for municipality in municipalities_li:
                    municipality_str = municipality.get_text()

                    # cleans the text
                    municipality_str = re.sub('\d|-- | --|   [ . ]|\n| -', '', municipality_str)
                    if municipality_str[0] == ' ':
                        municipality_str = municipality_str[1:]
                    if municipality_str[-1] == ' ':
                        municipality_str = municipality_str[:-1]

                    if len(municipality_str.split(' ')) > 1 and \
                            (municipality_str.split(' ')[0] == "Κοινότητα" or municipality_str.split(' ')[0] == "Δήμος"):
                        municipalities.append(municipality_str)

                        # Gets the Regions
                        # constructs the id of the element that contains the districts
                        munic_id = "--_" + municipality_str.replace(" ", "_") + "_--"
                        sub = sub_html.find('span', {"id": munic_id})
                        if sub is None:
                            # Many cases!
                            munic_id = municipality_str.replace(" ", "_")
                            sub = sub_html.find('span', {"id": munic_id})
                        if sub is None:
                            # Mεταξάδων case
                            munic_id = "--_" + municipality_str.replace(" ", "_") + "_-"
                            sub = sub_html.find('span', {"id": munic_id})

                        # finds the element that the districts names are located
                        flag = sub is None
                        while not flag and sub.name != 'dl':
                            sub = sub.next_element
                            flag = sub is None

                        districts = []
                        if not flag:
                            # gets the districts and cleans their text
                            for au in sub.find_all('dd'):
                                if au.find('b') is not None:
                                    district_str = au.find('b').get_text()

                                    # clean str
                                    district_str = re.sub('\d|\[|]', '', district_str)
                                    if district_str[0] == ' ':
                                        district_str = district_str[1:]
                                    if municipality_str[-1] == ' ':
                                        district_str = district_str[:-1]

                                    districts.append(district_str)
                                    print(district_str)

                        # insertion to the output
                        municipalities_districts[municipality_str] = pd.Series(districts)
                prefectures_municipalities[c_key] = pd.Series(municipalities)
        region_prefectures[r_key] = pd.Series(r_prefectures)

    # stores dictionaries into .csv
    rp = pd.DataFrame(region_prefectures)
    rp.to_csv("datasets/Kapodistrias_scheme/Regions_Prefectures.csv", sep='\t', columns=rp.columns, index=False)

    pm = pd.DataFrame(prefectures_municipalities)
    pm.to_csv("datasets/Kapodistrias_scheme/Prefectures_Municipalities.csv", sep='\t', columns=pm.columns, index=False)

    md = pd.DataFrame(municipalities_districts)
    md.to_csv("datasets/Kapodistrias_scheme/Municipalities_Districts.csv", sep='\t', columns=md.columns, index=False)
    exit()
    return rp, pm, md

