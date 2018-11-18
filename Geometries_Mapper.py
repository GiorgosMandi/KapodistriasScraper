import pandas as pd
from simpledbf import Dbf5
import unidecode
import time

# Δήμος Παλλήνης - Δήμος Κορώνειας - Δήμος Καλλιθέας - Δήμος Μεθώνης - Δήμος Νεάπολης Λασιθίου


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
    for index, e_id in enumerate(municipalities_wkt['ESYE_ID']):
        try:
            municipality_row = dbf.loc[dbf['ESYE_ID'] == str(e_id)]
            municipalities_label = municipality_row['GREEKNAME'].values[0]
            municipalities_prefecture = municipality_row['PREFECTURE'].values[0]

        except IndexError:
            e_id = "0" + str(e_id)
            municipality_row = dbf.loc[dbf['ESYE_ID'] == e_id]
            municipalities_label = municipality_row['GREEKNAME'].values[0]
            municipalities_prefecture = municipality_row['PREFECTURE'].values[0]


        # conflicts -- special occasions which demand special treatments
        '''if municipalities_label == "ΑΓΙΟΥ ΚΩΝΣΤΑΝΤΙΝΟΥ" :
            if dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΦΘΙΩΤΙΔΑΣ':
                municipalities_geometries['Δήμος Αγίου Κωνσταντίνου'] = municipalities_wkt['WKT'][index]
            else:
                municipalities_geometries['Κοινότητα Αγίου Κωνσταντίνου'] = municipalities_wkt['WKT'][index]

        elif municipalities_label == "ΑΓΙΟΥ ΓΕΩΡΓΙΟΥ":
            if dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΦΘΙΩΤΙΔΑΣ':
                municipalities_geometries['Δήμος Αγίου Γεωργίου Τυμφρηστού'] = municipalities_wkt['WKT'][index]
            elif dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΘΕΣΣΑΛΟΝΙΚΗΣ':
                municipalities_geometries['ΑΓΙΟΥ ΓΕΩΡΓΙΟΥ_1'] = municipalities_wkt['WKT'][index]
            else:
                municipalities_geometries['ΑΓΙΟΥ ΓΕΩΡΓΙΟΥ_2'] = municipalities_wkt['WKT'][index]

        elif municipalities_label == "ΚΑΛΛΙΘΕΑ":
            if dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΑΘΗΝΩΝ':
                municipalities_geometries['ΚΑΛΛΙΘΕΑ_1'] = municipalities_wkt['WKT'][index]
            elif dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΘΕΣΣΑΛΟΝΙΚΗΣ':
                municipalities_geometries['ΚΑΛΛΙΘΕΑ_2'] = municipalities_wkt['WKT'][index]
            else:
                municipalities_geometries['ΚΑΛΛΙΘΕΑ_3'] = municipalities_wkt['WKT'][index]

        if municipalities_label == "ΚΟΡΩΝΕΙΑΣ" :
            if dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΒΟΙΩΤΙΑΣ':
                municipalities_geometries['Δήμος Κορώνης'] = municipalities_wkt['WKT'][index]
            else:
                municipalities_geometries['Δήμος Κορώνειας'] = municipalities_wkt['WKT'][index]


        elif municipalities_label == "ΣΤΑΥΡΟΥΠΟΛΗΣ":
            if dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΘΕΣΣΑΛΟΝΙΚΗΣ':
                municipalities_geometries['Δήμος Σταυρουπόλεως'] = municipalities_wkt['WKT'][index]
            else:
                municipalities_geometries['Δήμος Σταυρούπολης'] = municipalities_wkt['WKT'][index]

        elif municipalities_label == "ΑΥΛΩΝΟΣ" :
            if dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΑΝΑΤΟΛΙΚΗΣ ΑΤΤΙΚΗΣ':
                municipalities_geometries[municipalities_label + "_1"] = municipalities_wkt['WKT'][index]
            elif dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΜΕΣΣΗΝΙΑΣ':
                municipalities_geometries[municipalities_label + "_2"] = municipalities_wkt['WKT'][index]
            elif dbf.loc[dbf['ESYE_ID'] == str(e_id)]['PREFECTURE'].values[0] == 'Ν. ΕΥΒΟΙΑΣ':
                municipalities_geometries[municipalities_label + "_3"] = municipalities_wkt['WKT'][index]
        else:'''
        if municipalities_label in municipalities_geometries:
            municipalities_geometries[municipalities_label].append((municipalities_prefecture, municipalities_wkt['WKT'][index]))
        else:
            municipalities_geometries[municipalities_label] = [(municipalities_prefecture, municipalities_wkt['WKT'][index])]


    subjects = []
    predicates = []
    objects = []

    entity_type = None
    entity_ID = None
    time.clock()
    aulona_count = 0
    error_counter = 0
    # Mapps the RDF entities with their Geometries
    for row in dataset.iterrows():

        # initialise varaibles
        if row[1]['Predicate'] == config['Predicates']['type']:
            entity_type = row[1]['Object']
        if row[1]['Predicate'] == config['Predicates']['kapodistrias_id']:
           entity_ID = row[1]['Object']
        if row[1]['Predicate'] == config['Predicates']['label']:
            entity_label = row[1]['Object']
        if row[1]['Predicate'] == config['Predicates']['upper_level']:
            entity_URI = row[1]['Subject']
            entity_upper_level = row[1]['Object'].split('/')[-1][:-1].replace("_", " ")

            print("Examining : ", entity_label)
            start_timer = time.time()


            # Maps the data
            # Regions' Geometries
            key = None
            if entity_type == config['Types']['regions']:
                key = entity_label[1:-1]

                # stores them in RDF
                geom_id = "<G_" + entity_ID[1:]
                subjects += [entity_URI, geom_id]
                predicates += [config['Predicates']['has_geometry'], config['Predicates']['asWKT']]
                objects += [geom_id, region_geometries[key]]
                region_geometries.pop(key)


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
                # stores them in RDF
                geom_id = "<G_" + entity_ID[1:]
                subjects += [entity_URI, geom_id]
                predicates += [config['Predicates']['has_geometry'], config['Predicates']['asWKT']]
                objects += [geom_id, prefectures_geometries[key]]
                prefectures_geometries.pop(key)



            # Municipalities' Geometries
            elif entity_type == config['Types']['municipalities']:

                # Most of the labels are the same in decoded formation
                # except the ones in the following IF statements
                if entity_label[1:-1] in config['Map_Dictionaries']:
                    key = config['Map_Dictionaries'][entity_label[1:-1]]
                elif entity_label[1:-1] in municipalities_geometries:
                    key = entity_label[1:-1]

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
                    error_counter += 1
                    print("Error: ", entity_label, "COUNT:", error_counter ,"\n\n")
                    continue

                # stores them in RDF
                geom_id = "<G_" + entity_ID[1:]
                subjects += [entity_URI, geom_id]
                predicates += [config['Predicates']['has_geometry'], config['Predicates']['asWKT']]
                if len(municipalities_geometries[key]) == 1:
                    objects += [geom_id, municipalities_geometries[key][0][1]]
                    municipalities_geometries.pop(key)
                else:
                    entity_upper_level = unidecode.unidecode(entity_label[1:-1].split(" ", 1)[1].upper())
                    if (entity_upper_level[:2] == 'AG' or entity_upper_level[:2] == 'NE') and len(entity_upper_level.split(" ")) >= 2:
                        entity_upper_level = entity_upper_level.split(" ")[1]

                    for prefectures in municipalities_geometries[key]:
                        prefecture_label = prefectures[0]
                        prefecture_label = unidecode.unidecode(prefecture_label[1:-1].split(" ", 1)[1].upper())
                        if (prefecture_label[:2] == 'AG' or prefecture_label[:2] == 'NE') and len(prefecture_label.split(" ")) >= 2:
                            prefecture_label = prefecture_label.split(" ")[1]
                            print("\t\t\t",prefecture_label,"--", entity_upper_level)

                        if prefecture_label == entity_upper_level:
                            objects += [geom_id, prefectures[1]]
                        elif LD(prefecture_label, entity_upper_level) < 3:
                            objects += [geom_id, prefectures[1]]
                            municipalities_geometries[key].remove(prefectures)

                print("\t", key, " -- duration:  %02d" % (time.time() - start_timer), "left: ",
                  len(municipalities_geometries), "\n\n")






    # stores the geometries in a CSV
    geometries = pd.DataFrame({'Subject': pd.Series(subjects),
                                'Predicate': pd.Series(predicates),
                                'Object': pd.Series(objects) })
    dataset = dataset.append(geometries)
    dataset.to_csv(path + "Kapodistrias_AD_G.csv", sep='\t', index=False)
    return dataset
