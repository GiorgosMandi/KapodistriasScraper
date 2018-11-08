import pandas as pd


# creates a dataset with all the administrative units that exist in
# "datasets/yago/yagoGeonamesTypes.tsv" file
def get_geonamesAD(filename = "datasets/yago/yagoGeonamesTypes.tsv"):
    skip_rows = 0
    step = 500000
    total_rows = 11591738
    administrative_divisions = pd.DataFrame()
    geoclasses = ["<geoclass_first-order_administrative_division>",
                  "<geoclass_second-order_administrative_division>",
                  "<geoclass_third-order_administrative_division>",
                  "<geoclass_fourth-order_administrative_division>"]
    while skip_rows < total_rows:

        geonames_data = pd.read_csv(filename, header=None, skiprows=skip_rows, nrows=step, sep='\t')
        administrative_divisions = administrative_divisions.append(geonames_data.loc[geonames_data[3].isin(geoclasses)][[1,2,3]])

        if skip_rows + step > total_rows:
            skip_rows += total_rows - skip_rows
        else:
            skip_rows += step

    print(administrative_divisions.shape)
    administrative_divisions.to_csv("datasets/yago/administrative_units.tsv",sep='\t', index=False)


get_geonamesAD()