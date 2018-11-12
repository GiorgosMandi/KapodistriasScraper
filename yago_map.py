import pandas as pd


# creates a dataset with all the administrative units that exist in
# "datasets/yago/yagoGeonamesTypes.tsv" file
def get_geonamesAD(config):
    filename = config['Geonames']['types_file']
    skip_rows = 0
    step = 500000
    administrative_divisions = pd.DataFrame()
    geoclasses = [config['Geonames']['first_order'],
                  config['Geonames']['second_order'],
                  config['Geonames']['third_order'],
                  config['Geonames']['fourth_order']]
    # reading dataset in chunks
    continue_flag = True
    while continue_flag: # skip_rows < total_rows:
        try:
            geonames_data = pd.read_csv(filename, header=None, skiprows=skip_rows, nrows=step, sep='\t')
        except pd.errors.EmptyDataError:
            continue_flag = False
        print(geonames_data.shape[0])
        administrative_divisions = administrative_divisions.append\
            (geonames_data.loc[geonames_data[3].isin(geoclasses)][[1,2,3]])

        if skip_rows + step > total_rows:
            skip_rows += total_rows - skip_rows
        else:
            skip_rows += step

    print("No Rows: ",administrative_divisions.shape[0])
    administrative_divisions.to_csv("datasets/yago/administrative_units.tsv",sep='\t', index=False)



import configparser
config = configparser.RawConfigParser()
config.read('config/config.ini')
get_geonamesAD(config)