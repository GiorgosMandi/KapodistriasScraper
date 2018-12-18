import pandas as pd
from simpledbf import Dbf5
import unidecode
import time
import csv



class Mapper:

    def __init__(self, configuration):
        self.config = configuration

    def __LD(self, s, t):
        if s == "":
            return len(t)
        if t == "":
            return len(s)
        if s[-1] == t[-1]:
            cost = 0
        else:
            cost = 1
        res = min([self.__LD(s[:-1], t) + 1, self.__LD(s, t[:-1]) + 1,
                   self.__LD(s[:-1], t[:-1]) + cost])
        return res


    def __label_preprocess(self, label, split=False):
        temp_label = unidecode.unidecode(label.upper())

        if split or label[:3] == "Ν. ":
            temp_label = temp_label.split(" ", 1)[1]
        if (temp_label[:2] == 'AG' or temp_label[:2] == 'NE') and len(label.split(" ")) >= 2:
            try:
                temp_label = temp_label.split(" ")[1]
            except IndexError:
                pass
        return temp_label


    def __dictionary_constructor(self, dbf, wkt, region=False, prefecture=False, municipality=False):
        id = None
        type = None
        upper_level = None
        if region:
            id = 'ESYE_ID'
            type = 'REGION'
        if prefecture:
            id = 'PREF_ID'
            type = 'PREFECTURE'
        if municipality:
            id = 'ESYE_ID'
            type = 'GREEKNAME'
            upper_level = 'PREFECTURE'

        dictionary = {}
        for index, e_id in enumerate(wkt[id]):
            try:
                row = dbf.loc[dbf[id] == str(e_id)]
                label = row[type].values[0]
            except IndexError:
                e_id = "0" + str(e_id)
                row = dbf.loc[dbf[id] == str(e_id)]
                label = row[type].values[0]
            # Special occasions
            if label in self.config['Map_Dictionaries']:
                label = self.config['Map_Dictionaries'][label]
            if municipality:
                upper_level_label = row[upper_level].values[0]
                if label in dictionary:
                    dictionary[label].append(
                        (upper_level_label, wkt['WKT'][index]))
                else:
                    dictionary[label] = [(upper_level_label, wkt['WKT'][index])]
            else:
                dictionary[label] = wkt['WKT'][index]

        return dictionary



    # Maps the labels of the dbf files and the produced dataset and inserts the
    # geometries in the dataset in WKT formation
    def Geometries_Mapper(self, dataset):
        dbf_file = self.config['File_Paths']['dbf_file']
        wkt_folder = self.config['File_Paths']['wkt_folder']
        path = self.config['File_Paths']['kapodistrias_folder']

        # The labels of Shapefile
        dbf = Dbf5(dbf_file, codec='ISO-8859-7')
        dbf = dbf.to_dataframe()

        # The Geometries in WKT form
        regions_wkt = pd.read_csv(wkt_folder + 'Regions_WKT.csv', encoding='ISO-8859-7')
        prefectures_wkt = pd.read_csv(wkt_folder + 'Prefectures_WKT.csv', encoding='ISO-8859-7')
        municipalities_wkt = pd.read_csv(wkt_folder + 'Municipalities_WKT.csv', encoding='ISO-8859-7')

        # Maps Regions' Geometries (WKT - Shapefile) and stores them in a dictionary
        region_geometries = self.__dictionary_constructor(dbf,  regions_wkt, region=True)

        # Maps Prefectures' Geometries (WKT - Shapefile) and stores them in a dictionary
        prefectures_geometries = self.__dictionary_constructor(dbf, prefectures_wkt, prefecture=True)

        # Maps Municipalities' Geometries (WKT - Shapefile) and stores them in a dictionary
        municipalities_geometries = self.__dictionary_constructor( dbf, municipalities_wkt, municipality=True)

        subjects = []
        predicates = []
        objects = []

        entity_type = None
        entity_ID = None
        entity_label = None
        time.clock()
        errors = []

        # Mapps the RDF entities with their Geometries
        for row in dataset.iterrows():

            # initialise varaibles
            if row[1]['Predicate'] == self.config['Predicates']['type']:
                entity_type = row[1]['Object']
            if row[1]['Predicate'] == self.config['Predicates']['kapodistrias_id']:
                entity_ID = row[1]['Object']
            if row[1]['Predicate'] == self.config['Predicates']['label']:
                entity_label = row[1]['Object'][1:-1]
            if row[1]['Predicate'] == self.config['Predicates']['upper_level']:
                entity_URI = row[1]['Subject']
                entity_upper_level = row[1]['Object'].split('/')[-1][:-1].replace("_", " ")

                print("Examining : ", entity_label)
                start_timer = time.time()


                key = None
                if entity_type == self.config['Types']['regions']:
                    area_dict = region_geometries
                elif entity_type == self.config['Types']['prefectures']:
                    area_dict = prefectures_geometries
                elif entity_type == self.config['Types']['municipalities']:
                    area_dict = municipalities_geometries
                else:
                    break

                # searches to find which geometry label matches with the entity's label
                if entity_label in self.config['Map_Dictionaries']:
                    key = self.config['Map_Dictionaries'][entity_label]
                elif entity_label in area_dict:
                    key = entity_label
                else:
                    temp_entity = self.__label_preprocess(entity_label, True)
                    for m_key in area_dict:
                        temp_key = self.__label_preprocess(m_key)
                        if temp_key == temp_entity:
                            key = m_key
                            break
                        else:
                            if temp_key[:3] == temp_entity[:3]:
                                distance = self.__LD(temp_key, temp_entity)
                                if distance < 3:
                                    key = m_key
                if key is None:
                    errors.append(entity_label)
                    print("Error 1: ", entity_label, "COUNT:", len(errors), "\n\n")
                    continue

                # stores them in RDF
                geom_id = "<kapo:G_" + entity_ID[6:]
                if entity_type == self.config['Types']['regions']:
                    objects += [geom_id, "\"" + region_geometries[key] + "\""]
                    subjects += [entity_URI, geom_id]
                    predicates += [self.config['Predicates']['has_geometry'], self.config['Predicates']['asWKT']]
                    region_geometries.pop(key)
                if entity_type == self.config['Types']['prefectures']:
                    objects += [geom_id, "\"" + prefectures_geometries[key] + "\""]
                    subjects += [entity_URI, geom_id]
                    predicates += [self.config['Predicates']['has_geometry'], self.config['Predicates']['asWKT']]
                    prefectures_geometries.pop(key)
                if entity_type == self.config['Types']['municipalities']:
                    if len(area_dict[key]) == 1:
                        objects += [geom_id, "\"" + municipalities_geometries[key][0][1] + "\""]
                        subjects += [entity_URI, geom_id]
                        predicates += [self.config['Predicates']['has_geometry'], self.config['Predicates']['asWKT']]
                        municipalities_geometries.pop(key)
                    else:
                        temp_entity_upper_level = entity_upper_level
                        entity_upper_level = self.__label_preprocess(entity_upper_level, True)
                        found = False
                        for prefectures in municipalities_geometries[key]:
                            prefecture_label = self.__label_preprocess(prefectures[0], True)
                            if prefecture_label == "PEIRAIOS KAI NESON":
                                prefecture_label = "PEIRAIOS"

                            print("-->\t\t", prefecture_label, "--", entity_upper_level)
                            found = prefecture_label == entity_upper_level
                            if not found and abs(len(prefecture_label) - len(entity_upper_level)) < 3:
                                found = self.__LD(prefecture_label, entity_upper_level) < 3
                            if found:
                                objects += [geom_id, "\"" + prefectures[1] + "\""]
                                subjects += [entity_URI, geom_id]
                                predicates += [self.config['Predicates']['has_geometry'],
                                               self.config['Predicates']['asWKT']]
                                municipalities_geometries[key].remove(prefectures)
                                break
                        if not found:
                            print("Error 2: ", entity_label, temp_entity_upper_level)

                print("\t", key, " -- duration:  %02d" % (time.time() - start_timer), "left: ",
                      len(municipalities_geometries), "\n\n")


        # stores the geometries in a CSV
        geometries = pd.DataFrame({'Subject': pd.Series(subjects),
                                   'Predicate': pd.Series(predicates),
                                   'Object': pd.Series(objects),
                                   'Ends' : pd.Series(["."] *len(objects))})
        dataset = dataset.append(geometries)
        dataset.to_csv(path + "Kapodistrias_AD_G.csv", sep='\t', header=None, index=False, quoting=csv.QUOTE_NONE,
                       encoding="UTF-8")
        print("Errors:\n")
        for error in errors:
            print("\t",error)
        print("\n\nRest Regions:")
        for key in region_geometries.keys():
            print("\t",key)
        print("\n\nRest Perfectures:")
        for key in prefectures_geometries.keys():
            print("\t", key)
        print("\n\nRest Municipalities:")
        for key in municipalities_geometries.keys():
            print("\t", key)
        return dataset