import pandas as pd


from simpledbf import Dbf5

def Mapper(dataset, dbf_file='datasets/Kapodistrias_scheme/Geometries/oria_kapodistriakwn_dhmwn.dbf'
           , wkt_folder='datasets/Kapodistrias_scheme/Geometries/WKT/',
           path='datasets/Kapodistrias_scheme/'):

    dbf = Dbf5(dbf_file, codec='ISO-8859-7')
    dbf = dbf.to_dataframe()

    regions_wkt = pd.read_csv(wkt_folder + 'Regions_WKT.csv', encoding='ISO-8859-7')
    perfectures_wkt = pd.read_csv(wkt_folder + 'Perfectures_WKT.csv', encoding='ISO-8859-7')
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
        print(index, region_label, str(e_id))


    print("\n\n\n")


    subjects = []
    predicates = []
    objects = []
    for row in dataset.iterrows():
        if row[0] == 100:
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

    geometries = pd.DataFrame({'Subject': pd.Series(subjects),
                            'Predicate': pd.Series(predicates),
                            'Object': pd.Series(objects)
                            })
    dataset = dataset.append(geometries)
    dataset.to_csv(path + "Kapodistrias_AU_G.csv", sep='\t', index=False)

