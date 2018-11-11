import pandas as pd
from simpledbf import Dbf5
import unidecode


# NOTE: there are divisions with the same name -- make key based on their id!


# Levenshtein distance for comparing strings
def LD(s, t):
    if s == "":
        return len(t)
    if t == "":
        return len(s)
    if s[-1] == t[-1]:
        cost = 0
    else:
        cost = 1
    res = min([LD(s[:-1], t) + 1, LD(s, t[:-1]) + 1,
               LD(s[:-1], t[:-1]) + cost])
    return res

# Maps the labels of the dbf files and the produced dataset and inserts the
# geometries in the dataset in WKT formation
def Mapper(config, dataset):
    dbf_file= config['File_Paths']['dbf_file']
    wkt_folder= config['File_Paths']['wkt_folder']
    path= config['File_Paths']['kapodistrias_folder']

    # The labels of Shapefile
    dbf = Dbf5(dbf_file, codec='ISO-8859-7')
    dbf = dbf.to_dataframe()

    # The Geometries in WKT form
    regions_wkt = pd.read_csv(wkt_folder + 'Regions_WKT.csv', encoding='ISO-8859-7')
    prefectures_wkt = pd.read_csv(wkt_folder + 'Perfectures_WKT.csv', encoding='ISO-8859-7')
    municipalities_wkt = pd.read_csv(wkt_folder + 'Municipalities_WKT.csv', encoding='ISO-8859-7')

    # Maps Regions' Geometries (WKT - Shapefile) and stores them in a dictionary
    region_geometries = {}
    for index, e_id in enumerate(regions_wkt['ESYE_ID']):
        try:
            region_label = dbf.loc[dbf['ESYE_ID'] == str(e_id)]['REGION'].values[0]
            # Special occasions
            if region_label in config['Map_Dictionaries']:
                region_label = config['Map_Dictionaries'][region_label]
        except IndexError:
            region_label = dbf.loc[dbf['ESYE_ID'] == "0" + str(e_id)]['REGION'].values[0]
        region_geometries[region_label] = regions_wkt['WKT'][index]


    # Maps Prefectures' Geometries (WKT - Shapefile) and stores them in a dictionary
    prefectures_geometries = {}
    for index, e_id in enumerate(prefectures_wkt['PREF_ID']):
        try:
            prefecture_label = dbf.loc[dbf['PREF_ID'] == str(e_id)]['PREFECTURE'].values[0]
        except IndexError:
            prefecture_label = dbf.loc[dbf['PREF_ID'] == "0" + str(e_id)]['PREFECTURE'].values[0]
        prefectures_geometries[prefecture_label] = prefectures_wkt['WKT'][index]


    # Maps Municipalities' Geometries (WKT - Shapefile) and stores them in a dictionary
    municipalities_geometries = {}
    flag = False
    for index, e_id in enumerate(municipalities_wkt['ESYE_ID']):
        try:

            municipalities_label = dbf.loc[dbf['ESYE_ID'] == str(e_id)]['GREEKNAME'].values[0]
        except IndexError:
            e_id = "0" + str(e_id)
            municipalities_label = dbf.loc[dbf['ESYE_ID'] == e_id]['GREEKNAME'].values[0]

        #special occasions
        if municipalities_label == "ΑΓΙΟΥ ΚΩΝΣΤΑΝΤΙΝΟΥ" :
            if dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΦΘΙΩΤΙΔΑΣ':
                municipalities_geometries['Δήμος Αγίου Κωνσταντίνου'] = municipalities_wkt['WKT'][index]
            else:
                municipalities_geometries['Κοινότητα Αγίου Κωνσταντίνου'] = municipalities_wkt['WKT'][index]

        elif municipalities_label == "ΑΥΛΩΝΟΣ" :
            if dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ':
                municipalities_geometries[municipalities_label + "_1"] = municipalities_wkt['WKT'][index]
            elif dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΜΕΣΣΗΝΙΑΣ':
                municipalities_geometries[municipalities_label + "_2"] = municipalities_wkt['WKT'][index]
            elif dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΕΥΒΟΙΑΣ':
                municipalities_geometries[municipalities_label + "_3"] = municipalities_wkt['WKT'][index]

        else:
            municipalities_geometries[municipalities_label] = municipalities_wkt['WKT'][index]


    subjects = []
    predicates = []
    objects = []

    entity_type = None
    entity_ID = None

    aulona_count = 0
    # Mapps the RDF entities with their Geometries
    for row in dataset.iterrows():

        # initialise varaibles
        if row[1]['Predicate'] == config['Predicates']['type']:
            entity_type = row[1]['Object']
        if row[1]['Predicate'] == config['Predicates']['kapodistrias_id']:
           entity_ID = row[1]['Object']
        if row[1]['Predicate'] == config['Predicates']['label']:
            entity_URI = row[1]['Subject']
            entity_label = row[1]['Object']
            print("Examining : ", entity_label)


            # Maps the data
            # Regions' Geometries
            key = None
            if entity_type == config['Types']['regions']:
                key = entity_label[1:-1]

            # Prefectures' Geometries
            elif entity_type == config['Types']['prefectures']:
                # Due to the fact that the labels of the datasets are different,
                # I use Levinstein distance in order to detect the same entities

                if entity_label[1:-1] in config['Map_Dictionaries']:
                    key = config['Map_Dictionaries'][entity_label[1:-1]]
                else:
                    temp_entity = unidecode.unidecode(entity_label[1:-1].split(" ", 1)[1].upper())
                    for p_key in prefectures_geometries:
                        temp_key = unidecode.unidecode(p_key.split(" ",1)[1])
                        if  temp_key[:3] != temp_entity[:3]:
                            continue
                        if temp_key == temp_entity:
                            key = p_key
                            break

                        else:
                            distance = LD(temp_key, temp_entity )
                            if distance < 3:
                                key = p_key
                                break

            # Municipalities' Geometries
            elif entity_type == config['Types']['municipalities']:
                # Most of the labels are the same in decoded formation
                # except the ones in the following IF statements
                if entity_label[1:-1] in config['Map_Dictionaries']:
                    key = config['Map_Dictionaries'][entity_label[1:-1]]
                #special occasions
                elif entity_label[1:-1] == "Δήμος Αυλώνα":
                    if aulona_count == 0:
                        key = 'ΑΥΛΩΝΟΣ_3'
                        aulona_count += 1
                    elif aulona_count == 1:
                        key = 'ΑΥΛΩΝΟΣ_2'

                else:
                    temp_entity = unidecode.unidecode(entity_label[1:-1].split(" ", 1)[1].upper())
                    if (temp_entity[:2] == 'AG' or temp_entity[:2] == 'NE') and len(temp_entity.split(" ")) >= 2:
                        temp_entity = temp_entity.split(" ")[1]

                    for m_key in municipalities_geometries:
                        temp_key = unidecode.unidecode(m_key)
                        if (temp_key[:2] == 'AG' or temp_key[:2] == 'NE') and len(temp_key.split(" ")) >= 2:
                            temp_key = temp_key.split(" ")[1]

                        if temp_key == temp_entity:
                            key = m_key
                            break
                        else:
                            if temp_key[:3] == temp_entity[:3]:
                                distance = LD(temp_key, temp_entity)
                                if distance < 3:
                                    key = m_key

            if key is None:
                print("--------------->ERROR: \t", entity_label)
                continue


            # stores them in dictionary
            geom_id = "<G_" + entity_ID[1:]
            subjects += [entity_URI, geom_id]
            predicates += [config['Predicates']['has_geometry'], config['Predicates']['asWKT']]
            if entity_type == config['Types']['regions']:
                objects += [geom_id, region_geometries[key]]
                region_geometries.pop(key)
            elif entity_type == config['Types']['prefectures']:
                objects += [geom_id, prefectures_geometries[key]]
                prefectures_geometries.pop(key)
            elif entity_type == config['Types']['municipalities']:
                objects += [geom_id, municipalities_geometries[key]]
                municipalities_geometries.pop(key)


    # stores the geometries in a CSV
    geometries = pd.DataFrame({'Subject': pd.Series(subjects),
                            'Predicate': pd.Series(predicates),
                            'Object': pd.Series(objects) })
    dataset = dataset.append(geometries)
    dataset.to_csv(path + "Kapodistrias_AD_G.csv", sep='\t', index=False)
    return dataset
