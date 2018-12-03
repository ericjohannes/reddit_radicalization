
import sys
# add my lib dir to the sys path
sys.path.insert(0, '../lib/')
# import my functions
import reddit_corpus

import pandas

subreddit_id = 't5_38unr' # for r/The_Donald

testlist = ['/l/research/social-media-mining/public/RC_2015-01-random-sample-1000000.jsonlines']

folder = '/l/research/social-media-mining/public/reddit/comments/'
file = 'RC_2018-08.xz'

infile = folder + file
outfile = '../data/filtered/' + file + '_' + subreddit_id

reddit_corpus.filter_posts(subreddit_id, infile, outfile, 'xz')

# create empty df, columns date start, date end, count of comments, count of commenters
columns_list = ['subreddit_id','start_date','end_date','comment_count','author_set']
# read 10k lines
# filter for r/the_donald

# add date time
# use authors_subs_counts



