import pandas as pd
import argparse
from wiki_parser import kapodistrias_au_parser, french_au_parser
from DatasetConstructor import  kapodistria_dataset_constructor, french_dataset_constructor
from Geometries_Mapper import Mapper

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
    import os
    print(os.path.basename(__file__))
    rc = pd.read_csv("datasets/Kapodistrias_scheme/Regions_Prefectures.csv", sep='\t')
    try:
        rc = pd.read_csv("datasets/Kapodistrias_scheme/Regions_Prefectures.csv", sep='\t')
        cm = pd.read_csv("datasets/Kapodistrias_scheme/Prefectures_Municipalities.csv", sep='\t')
        md = pd.read_csv("datasets/Kapodistrias_scheme/Municipalities_Districts.csv", sep='\t')
    except FileNotFoundError:
        rc, cm, md = kapodistrias_au_parser()
    dataset = kapodistria_dataset_constructor(rc, cm, md)
    Mapper(dataset)


if args.country == 'F':
    try:
        nr = pd.read_csv("datasets/French_scheme/New_Regions.csv", sep='\t')
        fr = pd.read_csv("datasets/French_scheme/Former_Regions.csv", sep='\t')
        ru = pd.read_csv("datasets/French_scheme/Remained.csv", sep='\t')
    except FileNotFoundError:
        nr, fr, ru = french_au_parser()
    french_dataset_constructor(nr, fr, ru)
