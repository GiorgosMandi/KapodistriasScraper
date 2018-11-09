import pandas as pd
import argparse
from wiki_parser import kapodistrias_ad_parser, french_ad_parser
from DatasetConstructor import  kapodistria_dataset_constructor, french_dataset_constructor
from Geometries_Mapper import Mapper

import configparser
config = configparser.RawConfigParser()
config.read('config/config.ini')

# main
parser = argparse.ArgumentParser(description='Choose country')
parser.add_argument('country', type=str, help='G for Greece, F for France', nargs='?', default='F')
args = parser.parse_args()

if args.country != 'F':
    print("GREECE")
    args.country = 'G'
else:
    print("FRANCE")

# if the files don't exist, it constructs them by fetching the data from
# their wikipedia pages -- Then it constructs the datasets in RDF form
if args.country == 'G':
    try:
        rc = pd.read_csv(config['File_Paths']['kapodistrias_folder'] + "Regions_Prefectures.csv", sep='\t')
        cm = pd.read_csv(config['File_Paths']['kapodistrias_folder'] + "Prefectures_Municipalities.csv", sep='\t')
        md = pd.read_csv(config['File_Paths']['kapodistrias_folder'] + "Municipalities_Districts.csv", sep='\t')
    except FileNotFoundError:
        rc, cm, md = kapodistrias_ad_parser(config)
    dataset = kapodistria_dataset_constructor(config, rc, cm, md)
    Mapper(dataset)


if args.country == 'F':
    try:
        nr = pd.read_csv(config['File_Paths']['french_folder'] + "New_Regions.csv", sep='\t')
        fr = pd.read_csv(config['File_Paths']['french_folder'] + "Former_Regions.csv", sep='\t')
        ru = pd.read_csv(config['File_Paths']['french_folder'] + "Remained.csv", sep='\t')
    except FileNotFoundError:
        nr, fr, ru = french_ad_parser(config)
    french_dataset_constructor(config, nr, fr, ru)
