# count hatespeech

import pandas as pd
from itertools import chain
from collections import defaultdict
import bz2
import concurrent.futures
import glob
import json
import datetime
import csv

print('start')
print(datetime.datetime.now())

# get list of hate terms. only without innocent meanings and only in english
terms_file = '../data/usedata/hatebase_vocab_woinnocent_eng_ebedits.csv'
with open(terms_file, 'r') as f:
  reader = csv.reader(f)
  terms_list = list(reader)

terms_set = set(terms_list[0])


# set subr ids
the_donald = 't5_38unr' # r/the_Donald,
democrats = 't5_2qn70' # r/Democrats
republican = 't5_2qndt' # r/republican
libertarian = 't5_2qh63' # r/libertarian
books = 't5_2qh4i' # r/books

subreddit_ids = [the_donald, democrats, republican, libertarian, books]
# testfile = '/l/research/social-media-mining/public/RC_201-01-random-sample-1000000.jsonlines'

# dir in which to save results
savedatadir = '../data/results'

# 
def set_in_files():
	"""wrapped up setting in-file names. Returns list of 24 strings."""
	indatadir = '/nobackup/ejblom/reddit'
	com_dir = '/comments'
	subm_dir = '/submissions'
	glob_end = '/filtered*'
	com_glob_str = indatadir + com_dir + glob_end
	subm_glob_str = indatadir + subm_dir + glob_end
	infilenames = sorted(glob.glob(com_glob_str)) + sorted(glob.glob(subm_glob_str))
	return infilenames


def tokenize(string, lowercase=False):
	"""Extract words from a string containing English words.
	Handling of hyphenation, contractions, and numbers is left to your
	discretion.
	Tip: you may want to look into the `re` module.
	Args:
	    string (str): A string containing English.
	    lowercase (bool, optional): Convert words to lowercase.
	Returns:
	    list: A list of words.
	"""
	import re
	words = re.findall("([\w'-+]+)", string)
	if lowercase == True:
		lowerwords = []
		for word in words:
			lowerwords.append(word.lower())
		words = lowerwords
	return words
    
def test_hate(setofbodytokens, setofterms):
	if len(setofbodytokens & setofterms) > 0:
		print(setofbodytokens & setofterms)
		return True
	else:
		return False

def find_hatespeech(filename):	
	hate_post_counts_list = []
	with open(filename) as workingfile:
		# create a dict for each line. it will be a row in a df
		for line in workingfile:
			post_dict = json.loads(line, strict=False)
			try:
				post_row = {'subreddit_id': post_dict['subreddit_id'], 'author': post_dict['author'], 'month': datetime.datetime.utcfromtimestamp(post_dict['created_utc']).month, 'post_count': 1,'hate_post_count': 0}
			except KeyError:
				print('couldn't init post row)
				print(post_dict)
				continue # can't do anything with it
			# try next thing
			try:
				if 'RC' in filename:
					tokens = tokenize(post_dict['body'], lowercase=True)
				else:
					tokens = tokenize(post_dict['selftext'], lowercase=True)
			except KeyError:
				print('error getting post text')
				print(post_dict)
				continue # can't do anything with it
			# go on
			if (test_hate(set(tokens), terms_set)):
				post_row['hate_post_count'] = 1
			hate_post_counts_list.append(post_row)
	hate_post_counts_df = pd.DataFrame(hate_post_counts_list)
	sum_df = hate_post_counts_df.groupby(['subreddit_id','month', 'author']).agg({'post_count': 'sum', 'hate_post_count': 'sum'})
	print('done')
	print(filename)
	print(datetime.datetime.now())
	return sum_df

filenames = set_in_files()

with concurrent.futures.ProcessPoolExecutor(max_workers=48) as executor:
	results = list(executor.map(find_hatespeech, filenames))

concat_results_df = pd.concat(results)
total_sum_df = concat_results_df.groupby(['subreddit_id','month', 'author']).agg({'post_count': 'sum', 'hate_post_count': 'sum'})
total_sum_df.to_csv("../data/results/hate_post_count.csv")
