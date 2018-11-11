
import sys
# add my lib dir to the sys path
sys.path.insert(0, '../lib/')
# import my functions
import reddit_corpus

subreddit_id = 't5_38unr' # for r/The_Donald

folder = '/l/research/social-media-mining/public/reddit/comments/'
file = 'RC_2018-09.xz'

infile = folder + file
outfile = '../data/filtered/' + file + '_' + subreddit_id

reddit_corpus.filter_posts(subreddit_id, infile, outfile, 'xz')


