# count comments in each sub by month

import pandas as pd
from itertools import chain
from collections import defaultdict
import bz2
import concurrent.futures
import glob
import json
import datetime

# r/the_Donald, r/Democrats, r/republican, r/libertarian, r/books
the_donald = 't5_38unr'
democrats = 't5_2qn70'
republican = 't5_2qndt'
libertarian = 't5_2qh63'
books = 't5_2qh4i'

subreddit_ids = [the_donald, democrats, republican, libertarian, books]
# testfile = '/l/research/social-media-mining/public/RC_201-01-random-sample-1000000.jsonlines'

savedatadir = ''

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

# test, it works

def test_infiles_access(somefilenames):
	for name in somefilenames:
		print(name)
		with open(name) as workingfile:
			print(workingfile.readline())

def get_post_counts(filename):	
	post_counts_list = []
	for id in subreddit_ids:
		for month in range(1,13):
			post_counts_list.append({'subreddit_id': id, 'month': month, 'count': 0})
	post_counts_df = pd.DataFrame(post_counts_list)
	with open(filename) as workingfile:
		for line in workingfile:
			post_dict = json.loads(line)
			if (post_dict['subreddit_id'] in subreddit_ids):
				post_counts_df.loc[(post_counts_df['subreddit_id'] == post_dict['subreddit_id']) & (post_counts_df['month'] == datetime.datetime.utcfromtimestamp(post_dict['created_utc']).month),'count'] += 1
	return post_counts_df


filenames = set_in_files()

with concurrent.futures.ProcessPoolExecutor(max_workers=48) as executor:
	# creats a list of what the function returns for each file
	results = list(executor.map(get_post_counts, filenames))

concat_results_df = pd.concat(results)

sum_df = concat_results_df.groupby(['subreddit_id','month']).agg({'count': 'sum'})

sum_df.to_csv("../results/post_count_subr_month.csv")
# it worked!