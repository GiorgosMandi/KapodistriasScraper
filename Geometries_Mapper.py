import pandas as pd
from simpledbf import Dbf5

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
    res = min([LD(s[:-1], t) + 1,
               LD(s, t[:-1]) + 1,
               LD(s[:-1], t[:-1]) + cost])
    return res

def Mapper(dataset, dbf_file='datasets/Kapodistrias_scheme/Geometries/oria_kapodistriakwn_dhmwn.dbf'
           , wkt_folder='datasets/Kapodistrias_scheme/Geometries/WKT/',
           path='datasets/Kapodistrias_scheme/'):

    dbf = Dbf5(dbf_file, codec='ISO-8859-7')
    dbf = dbf.to_dataframe()

    regions_wkt = pd.read_csv(wkt_folder + 'Regions_WKT.csv', encoding='ISO-8859-7')
    prefectures_wkt = pd.read_csv(wkt_folder + 'Perfectures_WKT.csv', encoding='ISO-8859-7')
    municipalities_wkt = pd.read_csv(wkt_folder + 'Municipalities_WKT.csv', encoding='ISO-8859-7')

    region_geometries = {}
    for index, e_id in enumerate(regions_wkt['ESYE_ID']):
        try:
            region_label = dbf.loc[dbf['ESYE_ID'] == str(e_id)]['REGION'].values[0]
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


    prefectures_geometries = {}
    for index, e_id in enumerate(prefectures_wkt['PREF_ID']):
        try:
            prefecture_label = dbf.loc[dbf['PREF_ID'] == str(e_id)]['PREFECTURE'].values[0]
        except IndexError:
            prefecture_label = dbf.loc[dbf['PREF_ID'] == "0" + str(e_id)]['PREFECTURE'].values[0]
        prefectures_geometries[prefecture_label] = prefectures_wkt['WKT'][index]



    print("\n\n-----------------------------------\n\n")


    subjects = []
    predicates = []
    objects = []

    entity_type = ''
    entity_ID = ''
    # Mapps the RDF entities with their Geometries
    for row in dataset.iterrows():
        if row[0] == 300:
            break
        if row[1]['Predicate'] == 'rdf:type':
            entity_type = row[1]['Object']
        if row[1]['Predicate'] == 'monto:hasKapodistrias_ID':
           entity_ID = row[1]['Object']

        if row[1]['Predicate'] == 'rdf:label':
            entity_URI = row[1]['Subject']
            entity_label = row[1]['Object']
            if entity_type == '<Region>':
                geom_id = "<G_" + entity_ID[1:]
                subjects += [entity_URI, geom_id]
                predicates += ['<monto:hasGeometry>','<monto:asWKT>' ]
                objects += [geom_id, region_geometries[entity_label[1:-1]]]
            elif entity_type == '<Prefecture>':
                distances = [LD(key.split(" ")[1], entity_label[1:-1].split(" ")[1].upper() ) for key in prefectures_geometries]
                min_index = distances.index(min(distances))
                print(entity_label, list(prefectures_geometries.keys())[min_index], distances[min_index])
                print("\n\n\n")



    # storing the the geometries in a CSV
    geometries = pd.DataFrame({'Subject': pd.Series(subjects),
                            'Predicate': pd.Series(predicates),
                            'Object': pd.Series(objects)
                            })
    dataset = dataset.append(geometries)
    dataset.to_csv(path + "Kapodistrias_AU_G.csv", sep='\t', index=False)

