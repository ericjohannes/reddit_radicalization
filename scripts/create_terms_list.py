# will read csv and output csv
import pandas as pd
import csv

infile = '../data/usedata/hatebase_vocab_woinnocent_eng.csv'
outfile = '../data/usedata/hatebase_vocab_woinnocent_eng_list.csv'
terms_df = pd.read_csv(infile)

mylist = list(terms_df.term)

with open(outfile, 'w', newline='') as myfile:
     wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
     wr.writerow(mylist)

list(terms_df.term)

