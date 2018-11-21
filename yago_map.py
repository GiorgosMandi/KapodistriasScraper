import pandas as pd
import rdflib as rdf
import configparser




# creates a dataset with all the administrative units that exist in
# "datasets/yago/yagoGeonamesTypes.tsv" file
def get_geonamesAD(config):
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
    administrative_divisions.to_csv(config['File_Paths']['yago_files'] +
                                    "administrative_units.tsv",sep='\t', index=False)
    return administrative_divisions


# finds the labels of the administrative units entities from
# the overall yago's geonames dataset
def get_geonamesLabels(config, dataset=None):
    filename = config['Geonames']['yago_dates']
    step = 1000000
    skiped_rows = 0
    administrative_divisions_labels = pd.DataFrame()
    if dataset is None:
        dataset = pd.read_csv(config['File_Paths']['yago_files']+"administrative_units.tsv", sep='\t', header=None)[0].values[1:]

    # reads dataset in chunks
    while True:
        try:
            geonames_data = pd.read_csv(filename, header=None, skiprows=skiped_rows, nrows=step, sep='\t')[[1,2,3]]
        except pd.errors.EmptyDataError:
            break
        skiped_rows += step
        administrative_divisions_labels.append(geonames_data.loc[geonames_data[1].isin(dataset)])
        print(administrative_divisions_labels)

    print("No Rows: ", administrative_divisions_labels.shape[0])
    administrative_divisions_labels.to_csv(config['File_Paths']['yago_files'] +
                                        "administrative_units_labels.tsv", sep='\t', index=False)



configs = configparser.RawConfigParser()
configs.read('config/config.ini')
#get_geonamesAD(configs)
get_geonamesLabels(configs)