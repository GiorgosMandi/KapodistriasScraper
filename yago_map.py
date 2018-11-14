import pandas as pd
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
def get_geonamesLables(config, dataset=None):
    filename = config['Geonames']['labels_file']
    step = 1000000
    skiped_rows = 0
    administrative_divisions_labels = pd.DataFrame()
    if dataset is None:
        dataset = pd.read_csv(config['File_Paths']['yago_files'] +
                              "administrative_units.tsv", sep='\t', header=None)[0].values

    # reads dataset in chunks
    while True:
        try:
            geonames_data = pd.read_csv(filename, header=None, skiprows=skiped_rows,
                                        nrows=step, sep='\t')
        except pd.errors.EmptyDataError:
            break

        skiped_rows += step
        print("Skiping ", skiped_rows)

        # finds the administrative units that were stored in dataset var,
        # and keeps their labels
        administrative_divisions_labels = administrative_divisions_labels.append \
            (geonames_data.loc[(geonames_data[2] == "rdfs:label") & (geonames_data[1].isin(dataset))])[[1,2,3]]

    print("No Rows: ", administrative_divisions_labels.shape[0])
    administrative_divisions_labels.to_csv(config['File_Paths']['yago_files'] +
                                        "administrative_units_labels.tsv", sep='\t', index=False)



configs = configparser.RawConfigParser()
configs.read('config/config.ini')
#get_geonamesAD(configs)
get_geonamesLables(configs)