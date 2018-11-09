import pandas as pd
import requests
import re
from bs4 import BeautifulSoup






def kapodistrias_ad_parser(config):
    path = config['File_Paths']['kapodistrias_folder']

    page = requests.get(config['Wiki_Paths']['kapodistrias_ad'])
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
            munic_page = requests.get(config['Wiki_Paths']['el_wiki'] + url['href'])
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
                munic_page = requests.get(config['Wiki_Paths']['el_wiki'] + url['href'])
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
                            # Most of the cases!
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
    rp.to_csv(path + "Regions_Prefectures.csv", sep='\t', columns=rp.columns,
              index=False)
    pm = pd.DataFrame(prefectures_municipalities)
    pm.to_csv(path + "Prefectures_Municipalities.csv", sep='\t', columns=pm.columns,
              index=False)
    md = pd.DataFrame(municipalities_districts)
    md.to_csv(path + "Municipalities_Districts.csv", sep='\t', columns=md.columns,
              index=False)
    return rp, pm, md


# ----------------------------------------------------------------------------------------------------------------------


def french_ad_parser(config):
    path = config['File_Paths']['french_folder']
    page = requests.get(config['Wiki_Paths']['french_ad'])
    soup = BeautifulSoup(page.content, 'html.parser')
    html = list(soup.children)[2]
    tables = html.find_all('table',  {"class": 'wikitable'})

    # Gets the Regions that didn't change
    remained_table = tables[1]
    data = remained_table.find_all('td')[1:]
    remained_units = {}
    for d in data:
        region_name = re.sub('\d|\(|\)|\n', '', d.get_text())
        if len(region_name) > 1:
            print(region_name)

            # gets Region's departments
            region_url = d.find('a', href=True)
            region_page = requests.get(config['Wiki_Paths']['en_wiki'] + region_url['href'])
            region_soup = BeautifulSoup(region_page.content, 'html.parser')
            region_html = list(region_soup.children)[2]
            department_table = region_html.find('ul', {'class': 'NavContent'})

            # Checkes whether the region got departments
            if department_table is not None:
                department_list = department_table.find_all('li')
                remained_units[region_name] = pd.Series([re.sub('\d|\(|\)|\n', '', d.get_text())
                                                         for d in department_list])
            else:
                remained_units[region_name] = pd.Series([])

    # Gets the Regions that were affected
    merged_table = tables[0]
    data = merged_table.find_all('td')[4:]

    new_departments = {}
    former_departments = {}
    merged_mapping = {}
    new_region = ""
    former_regions = []
    flag_c = 0
    for index, d in enumerate(data):
        region_name = re.sub('\d|\(|\)|\n', '', d.get_text())

        # parses the table
        if len(region_name) == 0:
            # Empty Row
            flag_c = 0
            # constructs a map that holds the information about who merged to produce who
            if len(former_regions) != 0:
                merged_mapping[new_region] = pd.Series(former_regions)
                new_region = ""
                former_regions = []
        else:
            flag_c += 1
            if flag_c == 2:
                # Interim region's name -- Not useful
                continue
            elif flag_c == 3:

                # New Regions
                print("\n------->", region_name)
                new_region = region_name

                # gets Region's departments
                region_url = d.find('a', href=True)
                region_page = requests.get(config['Wiki_Paths']['en_wiki'] + region_url['href'])
                region_soup = BeautifulSoup(region_page.content, 'html.parser')
                region_html = list(region_soup.children)[2]
                department_list = region_html.find('ul', {'class': 'NavContent'}).find_all('li')

                # cleans departments' names
                temp_list = []
                for dep in department_list:
                    dep_str = re.sub('\d|\(|\)|\n', '', dep.get_text())
                    if dep_str[-1] == ' ':
                        dep_str = dep_str[:-1]
                    print(dep_str)
                    temp_list.append(dep_str)
                new_departments[region_name] = pd.Series(temp_list)

            else:
                # Former Regions
                print("\n-->", region_name)
                former_regions.append(region_name)

                # gets Region's departments
                region_url = d.find('a', href=True)
                region_page = requests.get(config['Wiki_Paths']['en_wiki'] + region_url['href'])
                region_soup = BeautifulSoup(region_page.content, 'html.parser')
                region_html = list(region_soup.children)[2]
                department_list = region_html.find('ul', {'class': 'NavContent'}).find_all('li')

                for dep in department_list:
                    print(dep.get_text())

                former_departments[region_name] = pd.Series([re.sub('\d|\(|\)|\n', '', dep.get_text())
                                                             for dep in department_list])

    # stores dictionaries into .csv
    nr = pd.DataFrame(new_departments)
    nr.to_csv(path + "New_Regions.csv", sep='\t', columns=nr.columns, index=False)

    fr = pd.DataFrame(former_departments)
    fr.to_csv(path + "Former_Regions.csv", sep='\t', columns=fr.columns, index=False)

    mm = pd.DataFrame(merged_mapping)
    mm.to_csv(path + "Merged_Map.csv", sep='\t', columns=mm.columns, index=False)

    ru = pd.DataFrame(remained_units)
    ru.to_csv(path + "Remained.csv", sep='\t', columns=ru.columns, index=False)

    return nr, fr, ru
