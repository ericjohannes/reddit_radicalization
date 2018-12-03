#subreddit_id = 't5_38unr' # for r/The_Donald

import ../lib/reddit_corpus.py
# r/the_Donald, r/Democrats, r/republican, r/libertarian, r/books
subreddit_ids = 't5_2cneq' # r/Politics
filename = '/l/research/social-media-mining/public/RC_2015-01-random-sample-1000000.jsonlines'

datadir = '/nobackup/ejblom/reddit'

def subs_comments_counts(filename,subslist):
	reddit_dfs = pd.read_json(filename, lines=True, chunksize=10000)
	columns_list = ['subreddit_id','month','comment_count']
	subs_comments_df = pd.DataFrame(data=None, columns=columns_list) # empty df to return later
	for df in reddit_dfs:
		for sub in subslist:
			#new_sub_authors_dict = {}
			filtered_df = df[df['subreddit_id'] == sub]
			filtered_df = add_datetime(filtered_df)
			filtered_df = addmonth(filtered_df)
			grouped_df = filtered_df.groupby('month')
			for month, group in grouped_df:
				# should set the number of the week
				this_month = month
				# should get a set of authors
				#authors = set(group['author'].unique())
				# should get the number of comments, can add them later
				comment_count = len(group)
				# i think the first three values and sub id is already set
				row_dict = {'subreddit_id':sub, 'month':this_month, 'comment_count':comment_count}
	return subs_comments_df