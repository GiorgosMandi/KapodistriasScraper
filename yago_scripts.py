import pandas as pd
import configparser
import csv



# creates a dataset with all the administrative units that exist in
# "datasets/yago/yagoGeonamesTypes.tsv" file
def get_geonamesAU(config, produced_filename):
    filename = config['Geonames']['types_file']
    skiped_rows = 0
    step = 500000
    administrative_divisions = pd.DataFrame()
    geoclasses = [config['Geonames']['first_order'], config['Geonames']['second_order'],
                  config['Geonames']['third_order'], config['Geonames']['fourth_order']]

    # reads dataset in chunks
    while True:
        try:
            geonames_data = pd.read_csv(filename, header=None, skiprows=skiped_rows,
                                        nrows=step, sep='\t')
            print(geonames_data.shape[0])
        except pd.errors.EmptyDataError:
           break

        # stores only the entities that are administrative units
        administrative_divisions = administrative_divisions.append \
            (geonames_data.loc[geonames_data[3].isin(geoclasses)][[1,2,3]])

        skiped_rows += step

    print("No Rows: ",administrative_divisions.shape[0])
    administrative_divisions = administrative_divisions.assign(e=pd.Series(["."] * administrative_divisions.shape[0]).values)
    administrative_divisions.to_csv(config['File_Paths']['yago_files'] +
                                    produced_filename,sep='\t', index=False, header=None)
    return administrative_divisions


# finds the date facts of the entities in dataset
def get_yagoDateFacts(config,produced_filename, dataset=None):
    filename = config['Geonames']['yago_dates']
    step = 1000000
    skiped_rows = 0
    au_date_facts = pd.DataFrame()
    if dataset is None:
        dataset = pd.read_csv(config['File_Paths']['yago_files']+"produced/administrative_units.tsv", sep='\t', header=None)
    URIs = set(dataset[1].values[1:])

    # reads dataset in chunks
    while True:
        try:
            date_facts = pd.read_csv(filename, header=None, skiprows=skiped_rows, nrows=step, sep='\t')[[1,2,3]]
        except pd.errors.EmptyDataError:
            break
        skiped_rows += step
        au_date_facts = au_date_facts.append(date_facts.loc[date_facts[1].isin(URIs)])
        print("No Rows: ", len(au_date_facts))

        au_date_facts = au_date_facts.assign(e=pd.Series(["."] * au_date_facts.shape[0]).values)
    au_date_facts.to_csv(config['File_Paths']['yago_files'] + produced_filename, sep='\t', index=False, header=None)



def Strabon_requirements_adjustments(config, filename):
    editing_subject = lambda x: "<yago:" + x[1:]
    editing_property = lambda x: (f"<{x}>" if ":" in x else "<yago:" + x[1:] + ">") if x[0] != "<" else  (x if ":" in x else "<yago:" + x[1:])
    editing_object = lambda x: "<yago:" + x[1:] if  x[0] == "<" else f'"{ x.split("^",1)[0]}"'
    dataset = pd.read_csv(config['File_Paths']['yago_files'] + filename, header=None, sep='\t')[[0,1,2,3]]

    dataset[0] = dataset[0].apply(editing_subject)
    dataset[1] = dataset[1].apply(editing_property)
    dataset[2] = dataset[2].apply(editing_object)

    produced_file = config['File_Paths']['yago_files'] + filename.rsplit(".", 1)[0] + "_Strabon." +filename.rsplit(".",1)[-1]
    dataset.to_csv(produced_file, header=None, sep='\t', index=False, quoting=csv.QUOTE_NONE)
    print("The produced file is located in   :", produced_file )


configs = configparser.RawConfigParser()
configs.read('config/config.ini')
#adm_units = get_geonamesAU(configs, "produced/administrative_units.nt")
#get_yagoDateFacts(configs, "produced/administrative_units_datefacts.nt", dataset=adm_units)
Strabon_requirements_adjustments(configs, "produced/administrative_units_datefacts.nt")