import pandas as pd
from simpledbf import Dbf5
import unidecode

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

#
def Mapper(dataset, dbf_file='datasets/Kapodistrias_scheme/Geometries/oria_kapodistriakwn_dhmwn.dbf'
           , wkt_folder='datasets/Kapodistrias_scheme/Geometries/WKT/',
           path='datasets/Kapodistrias_scheme/'):

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
            if region_label == 'ΠΔΕ':
                region_label = 'Περιφέρεια Δυτικής Ελλάδας'
            if region_label == 'ΠΗ':
                region_label = 'Περιφέρεια Ηπείρου'
            if region_label == 'Π. Αν. Μακεδονίας κ Θράκης':
                region_label = 'Περιφέρεια Ανατολικής Μακεδονίας και Θράκης'
            if region_label == 'ΠΒΑ':
                region_label = 'Περιφέρεια Βορείου Αιγαίου'
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
            municipalities_label = dbf.loc[dbf['ESYE_ID'] == str(e_id)]['GREEKNAME'].values[0]
        except IndexError:
            municipalities_label = dbf.loc[dbf['ESYE_ID'] == "0" + str(e_id)]['GREEKNAME'].values[0]
        municipalities_geometries[municipalities_label] = municipalities_wkt['WKT'][index]


    subjects = []
    predicates = []
    objects = []

    entity_type = None
    entity_ID = None
    # Mapps the RDF entities with their Geometries
    for row in dataset.iterrows():

        # initialise varaibles
        if row[1]['Predicate'] == 'rdf:type':
            entity_type = row[1]['Object']
        if row[1]['Predicate'] == 'monto:hasKapodistrias_ID':
           entity_ID = row[1]['Object']
        if row[1]['Predicate'] == 'rdf:label':
            entity_URI = row[1]['Subject']
            entity_label = row[1]['Object']
            print("Examining : ", entity_label)


            # Maps the data
            # Regions' Geometries
            key = None
            if entity_type == '<Region>':
                key = entity_label[1:-1]

            # Prefectures' Geometries
            elif entity_type == '<Prefecture>':
                # Due to the fact that the labels of the datasets are different,
                # I use Levinstein distance in order to detect the same entities

                if entity_label[1:-1] ==  "Νομαρχία Πειραιώς":
                    key = 'Ν. ΠΕΙΡΑΙΩΣ ΚΑΙ ΝΗΣΩΝ'
                if entity_label[1:-1] == "Νομός Αιτωλίας και Ακαρνανίας":
                    key = 'Ν. ΑΙΤΩΛΟΑΚΑΡΝΑΝΙΑΣ'
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
            elif entity_type == '<Municipality>':
                # Most of the labels are the same in decoded formation
                # except the ones in the following IF statements
                if entity_label[1:-1] == "Δήμος Ιλίου":
                    key = "ΝΕΩΝ ΛΙΟΣΙΩΝ (ΙΛΙΟΥ)"
                elif entity_label[1:-1] == "Δήμος Μεταμορφώσεως":
                    key = "ΜΕΤΑΜΟΡΦΩΣΗΣ"
                elif entity_label[1:-1] == "Δήμος Αγίου Ιωάννου Ρέντη":
                    key = "ΑΓΙΟΥ ΙΩΑΝΝΗ ΡΕΝΤΗ"
                elif entity_label[1:-1] == "Δήμος Σαλαμίνος":
                    key = "ΣΑΛΑΜΙΝΑΣ"
                elif entity_label[1:-1] == "Δήμος Σπάτων-Λούτσας":
                    key = "ΣΠΑΤΩΝ - ΛΟΥΤΣΑΣ"
                elif entity_label[1:-1] ==  'Δήμος Παλαιού Φαλήρου':
                    key = "ΠΑΛΑΙΟΥ ΦΑΛΗΡΟΥ"
                elif entity_label[1:-1] ==  'Δήμος Καλυβίων Θορικού':
                    key = "ΚΑΛΥΒΙΩΝ ΘΟΡΙΚΟΥ"
                elif entity_label[1:-1] == "Κοινότητα Ανοίξεως":
                    key = "ΑΝΟΙΞΗΣ"
                elif entity_label[1:-1] == "Κοινότητα Μαλακάσης":
                    key = "ΜΑΛΑΚΑΣΑΣ"
                elif entity_label[1:-1] == "Κοινότητα Ροδοπόλεως":
                    key = "ΡΟΔΟΠΟΛΗΣ"
                elif entity_label[1:-1] == "Κοινότητα Σαρωνίδος":
                    key = "ΣΑΡΩΝΙΔΑΣ"
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
                print("ERROR: \t", entity_label)


            # stores them in dictionary
            geom_id = "<G_" + entity_ID[1:]
            subjects += [entity_URI, geom_id]
            predicates += ['<monto:hasGeometry>', '<monto:asWKT>']
            if entity_type == '<Region>':
                objects += [geom_id, region_geometries[key]]
                region_geometries.pop(key)
            elif entity_type == '<Prefecture>':
                objects += [geom_id, prefectures_geometries[key]]
                prefectures_geometries.pop(key)
            elif entity_type == '<Municipality>':
                objects += [geom_id, municipalities_geometries[key]]
                municipalities_geometries.pop(key)


    # stores the geometries in a CSV
    geometries = pd.DataFrame({'Subject': pd.Series(subjects),
                            'Predicate': pd.Series(predicates),
                            'Object': pd.Series(objects) })
    dataset = dataset.append(geometries)
    dataset.to_csv(path + "Kapodistrias_AU_G.csv", sep='\t', index=False)
    return dataset
