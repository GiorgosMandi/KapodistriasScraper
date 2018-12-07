import pandas as pd
import configparser
import csv



# creates a dataset with all the administrative units that exist in
# "datasets/yago/yagoGeonamesTypes.tsv" file
def get_geonamesAU(config, produced_filename):
    filename = config['yago']['types_file']
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



def map_yago_enities(src_filename, target_filename, produced_filename, target_dataset=None):
    step = 1000000
    skiped_rows = 0
    out_facts = pd.DataFrame()
    if target_dataset is None:
        target_dataset = pd.read_csv(target_filename, sep='\t', header=None)
    URIs = set(target_dataset[1].values[1:])

    # reads dataset in chunks
    while True:
        try:
            date_facts = pd.read_csv(src_filename, header=None, skiprows=skiped_rows, nrows=step, sep='\t')[[1,2,3]]
        except pd.errors.EmptyDataError:
            break
        skiped_rows += step
        out_facts = out_facts.append(date_facts.loc[date_facts[1].isin(URIs)])
        print("No Rows: ", len(out_facts))

        out_facts = out_facts.assign(e=pd.Series(["."] * out_facts.shape[0]).values)
    out_facts.to_csv( produced_filename, sep='\t', index=False, header=None)



# modifies the input dataset in order to be applied to Strabon
# in case of a big file the dataset is read in chunks
def Strabon_requirements_adjustments(config, filename, big_file=False):
    editing_subject = lambda x: "<yago:" + x[1:]
    editing_property = lambda x: (f"<{x}>" if ":" in x else f"<yago:{x[1:]}>") if x[0] != "<" else  (x if ":" in x else "<yago:" + x[1:])
    editing_object = lambda x: "<yago:" + x[1:] if  x[0] == "<" else f'"{ x.split("^",1)[0]}"'
    produced_file = config['File_Paths']['yago_files'] + filename.rsplit(".", 1)[0] + "_Strabon." + \
                    filename.rsplit(".", 1)[-1]

    if not big_file:
        dataset = pd.read_csv(config['File_Paths']['yago_files'] + filename, header=None, sep='\t')[[0,1,2,3]]
        dataset[0] = dataset[0].apply(editing_subject)
        dataset[1] = dataset[1].apply(editing_property)
        dataset[2] = dataset[2].apply(editing_object)
        dataset.to_csv(produced_file, header=None, sep='\t', index=False, quoting=csv.QUOTE_NONE)
    else:
        step = 1000000
        skiped_rows = 0
        while True:
            try:
                dataset = pd.read_csv(config['File_Paths']['yago_files'] + filename, header=None, skiprows=skiped_rows,
                                      nrows=step, sep='\t', quoting=csv.QUOTE_NONE)[[1, 2, 3]]
            except pd.errors.EmptyDataError:
                break
            print(skiped_rows)
            skiped_rows += step
            dataset[1] = dataset[1].apply(editing_subject)
            dataset[2] = dataset[2].apply(editing_property)
            #dataset[3] = dataset[3].apply(editing_object)
            dataset = dataset.assign(e=pd.Series(["."] * dataset.shape[0]).values)
            with open(produced_file, 'a') as f:
                dataset.to_csv(f, sep='\t', index=False, header=None, quoting=csv.QUOTE_NONE)

    print("The produced file is located in   :", produced_file )



config = configparser.RawConfigParser()
config.read('config/config.ini')
#adm_units = get_geonamesAU(configs, "produced/administrative_units.nt")
map_yago_enities(config['yago']['yago_dates'],
                 config['File_Paths']['yago_files'] + "produced/administrative_units.tsv",
                 config['File_Paths']['yago_files'] + "produced/administrative_units_datefacts.nt",
                 )
#Strabon_requirements_adjustments(configs, "produced/administrative_units_datefacts.nt")
#Strabon_requirements_adjustments(configs,"yagoLabels.tsv",  big_file=True)