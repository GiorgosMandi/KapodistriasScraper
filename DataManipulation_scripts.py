import pandas as pd
import csv
import os


import configparser
config = configparser.RawConfigParser()
config.read('config/config.ini')


# creates a dataset with all the administrative units that exist in
# "datasets/yago/yagoGeonamesTypes.tsv" file
def get_geonamesEntities(produced_filename, types):
    filename = '/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Datasets/YAGO/grc_data/geonamesClasses.tsv'
    geoclass_localitys = pd.DataFrame()

    # reads dataset in chunks
    skiped_rows = 0
    step = 500000
    while True:
        try:
            geonames_data = pd.read_csv(filename, header=None, skiprows=skiped_rows, nrows=step, sep='\t',
                                        quoting=csv.QUOTE_NONE)
        except pd.errors.EmptyDataError:
           break
        skiped_rows += step
        # stores only the entities that are administrative units
        geoclass_localitys = \
            geoclass_localitys.append(geonames_data.loc[geonames_data[3].isin(types)][[1,2,3]])

    print("No Rows: ",geoclass_localitys.shape[0])
    geoclass_localitys = geoclass_localitys.assign(e=pd.Series(["."] * geoclass_localitys.shape[0]).values)
    geoclass_localitys.to_csv(config['File_Paths']['yago_files'] + produced_filename, sep='\t', index=False,
                                    header=None, quoting=csv.QUOTE_NONE)
    return geoclass_localitys


# gets the cordinates of the entities that exist in target_filename
def get_locationAU(target_filename, produced_filename):
    if os.path.exists(produced_filename):
        os.remove(produced_filename)
    src_filename = config['yago']['yago_literals']
    properties = [config['Geonames']['has_lat'], config['Geonames']['has_long'],
                  config['Geonames']['has_label'],config['Geonames']['located']]

    target_dataset = pd.read_csv(target_filename, sep='\t', header=None,  quoting=csv.QUOTE_NONE)
    URIs = set(target_dataset[0].values[1:])
    total = 0
    skiped_rows = 0
    step = 1000000
    while True:
        try:
            geonames_data = pd.read_csv(src_filename, header=None, skiprows=skiped_rows, nrows=step, sep='\t',
                                        quoting=csv.QUOTE_NONE)
        except pd.errors.EmptyDataError:
           break
        skiped_rows += step
        facts = geonames_data.loc[geonames_data[1].isin(URIs)]
        results = facts.loc[facts[2].isin(properties)][[1, 2, 3]]
        results = results.assign(e=pd.Series(["."] * results.shape[0]).values)
        with open(produced_filename, 'a') as f:
            results.to_csv(f, sep='\t', index=False, header=None, quoting=csv.QUOTE_NONE)
        total += len(results)
        print("No Rows: ", total, "/", skiped_rows)


# Given two dataset, finds the facts of the URIs of target that exist in src
def map_yago_enities(target_filename, src_filename, produced_filename, target_dataset=None, property=None, object=None, indexes= [1,2,3]):
    if os.path.exists(produced_filename):
        os.remove(produced_filename)
    step = 1000000
    skiped_rows = 0
    out_facts = pd.DataFrame()
    total = 0
    if target_dataset is None:
        target_dataset = pd.read_csv(target_filename, sep='\t', header=None,  quoting=csv.QUOTE_NONE)
    if property is not None:
        target_dataset = target_dataset.loc[(target_dataset[1]==property) & (target_dataset[2]==object)]
    URIs = set(target_dataset[0].values)
    # reads dataset in chunks
    while True:
        try:
            facts = pd.read_csv(src_filename, header=None, skiprows=skiped_rows, nrows=step, sep='\t',
                                     quoting=csv.QUOTE_NONE)[indexes]
        except pd.errors.EmptyDataError:
            break
        skiped_rows += step
        out_facts = facts.loc[facts[1].isin(URIs)]
        out_facts = out_facts.assign(e=pd.Series(["."] * out_facts.shape[0]).values)
        total += out_facts.shape[0]
        print("No Rows: ", total, "/", skiped_rows)
        if total > 0:
            with open(produced_filename, 'a') as f:
                out_facts.to_csv(f, sep='\t', index=False, header=None, quoting=csv.QUOTE_NONE)
    return out_facts



# modifies the input dataset in order to be applied to Strabon
# in case of a big file the dataset is read in chunks
def Strabon_requirements_adjustments(filename, produced_filename, big_file=False):
    editing_subject = lambda x: "<yago:" + x[1:]
    editing_property = lambda x: (f"<{x}>" if ":" in x else f"<yago:{x[1:]}>") if x[0] != "<" else  (x if ":" in x else "<yago:" + x[1:])
    editing_object = lambda x: "<yago:" + x[1:] if  x[0] == "<" else (f'"{ x.split("^",1)[0]}"' if x[0]!='\"' else x)

    if not big_file:
        dataset = pd.read_csv(filename, header=None, sep='\t', quoting=csv.QUOTE_NONE)[[0,1,2,3]]
        dataset[0] = dataset[0].apply(editing_subject)
        dataset[1] = dataset[1].apply(editing_property)
        dataset[2] = dataset[2].apply(editing_object)
        dataset.to_csv(produced_filename, header=None, sep='\t', index=False, quoting=csv.QUOTE_NONE)
    else:
        if os.path.exists(produced_filename):
            os.remove(produced_filename)
        step = 1000000
        skiped_rows = 0
        while True:
            try:
                dataset = pd.read_csv(filename, header=None, skiprows=skiped_rows,nrows=step, sep='\t'
                                      , quoting=csv.QUOTE_NONE)[[1, 2, 3]]
            except pd.errors.EmptyDataError:
                break
            print(skiped_rows)
            skiped_rows += step
            dataset[0] = dataset[0].apply(editing_subject)
            dataset[1] = dataset[1].apply(editing_property)
            dataset[2] = dataset[2].apply(editing_object)
            dataset = dataset.assign(e=pd.Series(["."] * dataset.shape[0]).values)
            with open(produced_filename, 'a') as f:
                dataset.to_csv(f, sep='\t', index=False, header=None, quoting=csv.QUOTE_NONE)

    print("The produced file is located in   :", produced_filename )


def dataset_repair(filename):
    dataset = pd.read_csv(filename, header=None, sep='\t', quoting=csv.QUOTE_NONE)
    Subject = []
    Object = []
    Predicate = []
    target = ['rdfs:label', 'skos:prefLabel', 'monto:asWKT', 'yago:hasLatitude', 'yago:hasLongitude',
              'monto:wasDestroyedOnDate','monto:wasCreatedOnDate','monto:asWKT', 'monto:hasKapodistrias_Type'
              ,'monto:hasKapodistrias_ID']
    for index,row in dataset.iterrows():
        #row[0] = row[0].replace("yago:","")
        #row[2] = row[2].replace("yago:","")
        if row[0][0] != "<": row[0] = f"<{row[0]}>"
        if row[1][0] != "<": row[1] = f"<{row[1]}>"
        try:
            if row[1][1:-1] in target:
                if row[2][0] != "\"":
                    row[2] = f'"{row[2]}".'
            else:
                if row[2][0] != "<": row[2] = f"<{row[2]}>."
        except TypeError:
            print(row)
            print(index)
            continue
        Subject.append(row[0])
        Predicate.append(row[1])
        Object.append(row[2])
    fixed_dataset = pd.DataFrame({'s' : pd.Series(Subject),
                                  'p' : pd.Series(Predicate),
                                  'o' : pd.Series(Object)})
    fixed_dataset.to_csv(filename, sep='\t', index=False, header=None, quoting=csv.QUOTE_NONE)

# Concat multiple datasets into one
def concat(filenames, produced_filename):
    out = pd.DataFrame()
    for file in filenames:
        try:
            dataset = pd.read_csv(file, header=None, sep='\t', quoting=csv.QUOTE_NONE)
            out = out.append(dataset)
            print(out.shape[0])
        except FileNotFoundError:
            continue
    out.to_csv(produced_filename, sep='\t', index=False, header=None, quoting=csv.QUOTE_NONE)


# Give to entities the geometries of their upper class
# The geometries of the upper classes must be located in the file_source
def geometry_insertion(filename, file_source, produced_filename):
    geom_uri = {}
    uri_to_geom = {}
    geometry_dataset = pd.read_csv(file_source, header=None, sep='\t', quoting=csv.QUOTE_NONE)[[0, 1, 2]]
    for index, row in geometry_dataset.iterrows():
        if row[1] == config['Predicates']['has_geometry']:
            geom_uri[row[2][:-1]] = row[0]
        elif row[1] ==  config['Predicates']['asWKT']:
            uri_to_geom[geom_uri[row[0]]] = row[2]

    geom_uri = {}
    dataset = pd.read_csv(filename, header=None, sep='\t', quoting=csv.QUOTE_NONE)[[0, 1, 2, 3]]
    produced = pd.DataFrame()
    for index, row in dataset.iterrows():
        produced = produced.append(row)

        if row[1] == config['Predicates']['kapodistrias_id']:
            geom_id = '<Geometry_' + row[2][1:-1] + '>'
            geom_uri[row[0]] = geom_id
            produced = produced.append(
                pd.Series({0: row[0], 1: config['Predicates']['has_geometry'], 2: geom_id, 3: '.'})
                , ignore_index=True)

        elif row[1] == config['Predicates']['upper_level']:
            try:
                produced = produced.append(
                 pd.Series({0: geom_uri[row[0]], 1: config['Predicates']['asWKT'], 2: uri_to_geom[row[2]][:-1], 3: '.'})
                 , ignore_index=True)
            except KeyError:
                pass
    produced.to_csv(produced_filename, sep='\t', index=False, header=None, quoting=csv.QUOTE_NONE)






'''
print("geoclass_locality")
get_geonamesEntities("geoclass_locality/gr_geoclass_locality.nt", ['<geoclass_locality>'])

print("\n\nPP literalFacts\n")
map_yago_enities(config['File_Paths']['yago_files'] + "geoclass_locality/gr_geoclass_locality.nt",
                 config['File_Paths']['yago_files'] + "grc_data/literalFacts.tsv",
                 config['File_Paths']['yago_files'] + "geoclass_locality/literalFacts.nt")

print("\n\nPP GeonamesData\n")
map_yago_enities(config['File_Paths']['yago_files'] + "geoclass_locality/gr_geoclass_locality.nt",
                 config['File_Paths']['yago_files'] + "grc_data/geonamesData.tsv",
                 config['File_Paths']['yago_files'] + "geoclass_locality/GeonamesData.nt")

print("\n\nPP labels\n")
map_yago_enities(config['File_Paths']['yago_files'] + "geoclass_locality/gr_geoclass_locality.nt",
                 config['File_Paths']['yago_files'] + "grc_data/labels.tsv",
                 config['File_Paths']['yago_files'] + "geoclass_locality/labels.nt")

print("\n\nPP DateFacts\n")
map_yago_enities(config['File_Paths']['yago_files'] + "geoclass_locality/gr_geoclass_locality.nt",
                 config['File_Paths']['yago_files'] + "yagoDateFacts.tsv",
                 config['File_Paths']['yago_files'] + "geoclass_locality/dateFacts.nt")


concat([config['File_Paths']['yago_files'] + "geoclass_locality/literalFacts.nt",
        config['File_Paths']['yago_files'] + "geoclass_locality/GeonamesData.nt",
        config['File_Paths']['yago_files'] + "geoclass_locality/labels.nt",
        config['File_Paths']['yago_files'] + "geoclass_locality/dateFacts.nt",
        config['File_Paths']['yago_files'] + "geoclass_locality/gr_localities"],
       config['File_Paths']['yago_files'] + "geoclass_locality/gr_geoclass_locality.tsv")
'''



def map_datefacts(dataset_location, bothdataset_location, onlydataset_location, option):
    dataset = pd.read_csv(dataset_location,  sep="\t", header=None,  quoting=csv.QUOTE_NONE)
    URIs = dataset[0].values
    URIs =  list(dict.fromkeys(URIs))

    both = pd.read_csv(bothdataset_location, sep=" ", header=None)[0].values
    only = []
    try:
        only = pd.read_csv(onlydataset_location, sep=" ", header=None)[0].values
    except pd.errors.EmptyDataError:
        pass

    datefacts = pd.DataFrame();
    for uri in URIs :

        if uri in both:
            datefacts = datefacts.append(pd.Series({0: uri,
                              1: "<http://kr.di.uoa.gr/yago-extension/ontology/officialCreationDate>",
                              2: "\"1997-##-##\"^^<http://www.w3.org/2001/XMLSchema#date>",
                              3: "."}), ignore_index=True)
        else:
            if option == "kapo":
                if uri in only:
                    datefacts = datefacts.append(pd.Series({0: uri,
                                      1: "<http://kr.di.uoa.gr/yago-extension/ontology/officialCreationDate>",
                                      2: "\"1997-##-##\"^^<http://www.w3.org/2001/XMLSchema#date>",
                                      3: "."}), ignore_index=True)
                    datefacts = datefacts.append(pd.Series({0: uri,
                                      1: "<http://kr.di.uoa.gr/yago-extension/ontology/officialTerminationDate>",
                                      2: "\"2011-##-##\"^^<http://www.w3.org/2001/XMLSchema#date>",
                                      3: "."}), ignore_index=True)
            elif option == "kalli":
                if uri in only:
                    datefacts = datefacts.append(pd.Series({0: uri,
                                      1: "<http://kr.di.uoa.gr/yago-extension/ontology/officialCreationDate>",
                                      2: "\"2011-##-##\"^^<http://www.w3.org/2001/XMLSchema#date>",
                                      3: "."}), ignore_index=True)
    result = dataset.append(datefacts)

    result.to_csv("/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/Kapodistrias/municipalities/matched.nt",
                  sep='\t', index=False, header=None, quoting=csv.QUOTE_NONE, escapechar='\\',
                )




#map_datefacts("/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/Kapodistrias/GRC_mundist-ppl_matches/matched.nt",
#              "/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/merged/FourthOrder/both",
#              "/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/merged/FourthOrder/onlyKapodistrias"
#             ,"kapo")

#map_datefacts("/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/Kallikratis/kallikratisMunUnitsCommunities/matched.nt",
#              "/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/merged/FourthOrder/both",
#              "/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/merged/FourthOrder/onlyKallikratis"
#             ,"kalli")

map_datefacts("/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/Kapodistrias/municipalities/matched_noDateFacts.nt",
              "/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/helping_files/Municipalitites/both",
              "/home/giorgosmandi/Documents/DIT/Thesis/Yago_Extension/Results/helping_files/Municipalitites/onlyKapodistrias"
             ,"kapo")


