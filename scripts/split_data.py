# split the data 
import ../lib/reddit_corpus.py
import json
import re
import json
import concurrent.futures
import glob
import bz2

# r/the_Donald, r/Democrats, r/republican, r/libertarian, r/books
the_donald = 't5_38unr'
democrats = 't5_2qn70'
republican = 't5_2qndt'
libertarian = 't5_2qh63'
books = 't5_2qh4i'

subreddit_ids = [the_donald, democrats, republican, libertarian, books]
# testfile = '/l/research/social-media-mining/public/RC_201-01-random-sample-1000000.jsonlines'

savedatadir = '/nobackup/ejblom/reddit'
# test_dir = '/nobackup/ejblom/reddit/test'

indatadir = '/l/research/social-media-mining/public/reddit'
commentdir = '/comments'
submissionsdir = '/submissions'
glob_end = '/RC_2017-*.bz2'
glob_str = indatadir + commentdir + glob_end

filenames = sorted(glob.glob(glob_str))

def filter_subs(filename):
	match = re.search(r"(RC_2017-.{2})", filename)	
	newfile = savedatadir + commentdir + '/filtered-' + match.group(1) + '.jsonlines'
	with open(newfile, 'a+') as outfile, bz2.open(filename, 'rt') as infile:
		for line in infile:
			test_json = json.loads(line)
			if (test_json['subreddit_id'] in subreddit_ids):
				outfile.write(line)

def filter_subs_compressed(filename):
	match = re.search(r"(RC_2017-.*)", filename)	
	newfile = savedatadir + commentdir + '/filtered-' + match.group(1) + '.jsonlines'
	with bz2.open(newfile, 'a+') as outfile, bz2.open(filename, 'rt') as infile:
		for line in infile:
			test_json = json.loads(line)
			if (test_json['subreddit_id'] in subreddit_ids):
				outfile.write(line)

with concurrent.futures.ProcessPoolExecutor(max_workers=48) as executor:
	results = list(executor.map(filter_subs, filenames))

# with concurrent.futures.ProcessPoolExecutor(max_workers=48) as executor:
# 	results = list(executor.map(filter_subs_compressed, filenames))
# timeit.timeit('match = re.search(r"subreddit_id\":\"(.{8})", testcomment)', number=10000)
# reg_str = r'subreddit_id":"(.{8})'

# timeit.timeit("re.search(re.search(r'subreddit_id\":\"(.{8})', testcomment)", setup='import re', number=10000)

# timeit.timeit('test_json = json.loads(testcomment)', number=10000)