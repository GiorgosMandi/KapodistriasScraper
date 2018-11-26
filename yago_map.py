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


# finds the date facts of the entities in dataset
def get_yagoDateFacts(config,produced_filename, dataset=None):
    filename = config['Geonames']['yago_dates']
    step = 1000000
    skiped_rows = 0
    if dataset is None:
        dataset = pd.read_csv(config['File_Paths']['yago_files']+"administrative_units.tsv", sep='\t'
                              , header=None)[0].values[1:]
    date_facts = []
    # reads dataset in chunks
    while True:
        try:
            geonames_data = pd.read_csv(filename, header=None, skiprows=skiped_rows, nrows=step, sep='\t')[[1,2,3]]
        except pd.errors.EmptyDataError:
            break
        skiped_rows += step

        date_facts += list(set(geonames_data[1].values).intersection(set(dataset)))
        print(len(date_facts))

    print("No Rows: ", len(date_facts))
    df = pd.DataFrame({"Date Facts" : pd.Series(date_facts)})
    df.to_csv(config['File_Paths']['yago_files'] + produced_filename, sep='\t', index=False)


# finds yago triplets that their URIs exist in dataset
def get_yago_triplets(config,produced_filename, dataset=None):

    filename = config['File_Paths']['yago_files'] + "yagoFacts.tsv"
    step = 1000000
    skiped_rows = 0
    results = pd.DataFrame()
    if dataset is None:
        dataset = pd.read_csv(config['File_Paths']['yago_files'] + "administrative_units_datefacts.tsv", sep='\t'
                              , header=None)[0].values
    while True:
        try:
            yago_data = pd.read_csv(filename, header=None, skiprows=skiped_rows, nrows=step, sep='\t')[[0, 1, 2, 3]]
        except pd.errors.EmptyDataError:
            break
        skiped_rows += step
        results = results.append(yago_data.loc[yago_data[1].isin(dataset)])
        print(skiped_rows, "\t", len(results))

    print("No Rows: ", len(results))
    results.to_csv(config['File_Paths']['yago_files'] + produced_filename
              , sep='\t', index=False)


configs = configparser.RawConfigParser()
configs.read('config/config.ini')
#get_geonamesAD(configs)
#get_yagoDateFacts(configs, "administrative_units_datefacts.tsv")
get_yago_triplets(configs, "AdministrativeUnits_DateFacts.tsv")