# library for dealing with reddit corpus in python

# to use this, at the top of a script put 
# import sys
# sys.path.insert(0, 'path/to/dir/with/this/file')
# path can be like '../lib/'
# import reddit_corpus
import lzma


def filter_posts(subreddit_id, inputfile, outputfile, filetype):
	"""
	Fast way to filter large reddit corpus data for posts on specific
	subreddit. Reads input file. for each line, if if lines contains subreddit_id,
	line is saved to outputfile.

	subreddit_id = id of subreddit in bae 36
	inputfile = name of file you read from (from reddit corpus)
	outputfile = name of file to which filtered comments are saved
	filetype = use 'xz' if it's xz, anything else goes to default
	"""
	# should open subreddits file and create new file we append to

	if filetype is 'xz':
		with lzma.open(inputfile, mode='rt') as infile, lzma.open(outputfile, 'w') as outfile:
			for line in infile:
				if subreddit_id in line:
					outfile.write(line.encode())
	else:
		with open(inputfile, 'r') as infile, open(outputfile, 'a+') as outfile:
			for line in infile:
				if subreddit_id in line:
					outfile.write(line)

def sorting_hat(subreddit_ids, inputfile, savepath):
	"""filter lines of inputfile for subreddits listed by id. Put them in files
	named by savepath + subreddit_id + inputfile """

	filedict = {}
	for sub in subreddit_ids:
		outputfile = 'savepath' + 'sub' + '.xz'
		filedict[sub] = lzma.open(outputfile, 'a+')

	with lzma.open(inputfile, mode='rt') as infile:
		for line in infile:
			for sub in subreddit_ids:
				if sub in line:
					filedict[sub].write(line)

	for sub in subreddit_ids:
		filedict[sub].close()

def add_datetime(somedf):
	"""converts the created utc column into a datetime object in pandas. returns
	nothing. the df you give the function is changed."""
	somedf['datetime'] = pd.to_datetime(somedf['created_utc'], unit='s')

# to compare dates: datetime.date(yyy, m, day)
# datetime.date(2011, 1, 1)
def choose_fortnight(somedatetime):
	dates = []
	for x in range(6):
		dates.append(datetime.date(2015, 1, 1) + timedelta(x * 14))
	for x in dates:
		if somedatetime.date() < x:
			return dates.index(x)

def choose_week(somedatetime):
	dates = []
	for x in range(1:54):
		dates.append(datetime.date(2015, 1, 1) + timedelta(x * 7))
	for x in dates:
		if somedatetime.date() < x:
			return dates.index(x)

def add_week(somedf):
	somedf['week'] = somedf['datetime'].apply(choose_week)
	return somedf

def add_month(somedf):
	"""takes a df and modies it. adds a month column labled 'month'
	df must have a 'datetime' column."""
	somedf['month'] = somedf['datetime'].dt.month
	return somedf

def fortnight_subreddit_totals(somedf, subslist):
	"""
	takes a dataframe of comment data. returns a dict where keys are the ids of top
	100 subreddits, values are another dict. Second level dict, keys are fortnights,
	and the values are the number of comments in that subreddit in that fortnight
	"""
	top100_list = subslist
	top100_total = []
	totals = {}
	# add fortnight
	somedf['fortnight'] = somedf['datetime'].apply(choose_fortnight)
	# flter for top 100
	filtered_df = somedf[somedf['subreddit_id'].isin(top100_list)]
	# sum by fornight for total
	for x in range(6):
		top_dict = len(filtered_df[filtered_df['fortnight'] == x])
		top100_total.append(top_dict)
	totals['total'] = top100_total
	# sum by fortnight by subreddit
	for subreddit in top100_list:
		# all subs come out with the same numbers. the result of len above and below is always the same number
		# on a test of one df from
		subreddit_list = []
		sliced_df = filtered_df[filtered_df['subreddit_id'] == subreddit]
		for x in range(6):
			sub_dict = len(sliced_df[sliced_df['fortnight'] == x])
			subreddit_list.append(sub_dict)
		totals[subreddit] = subreddit_list
	return totals

def initialize_total_dict(subslist):
	new_total_dict = {}
	empty_list = [0,0,0,0,0,0]
	new_total_dict['total'] = empty_list
	for sub in subslist:
		new_total_dict[sub] = empty_list
	return new_total_dict

def first_round_combo(fileslist):
	reddit_dfs = pd.read_json(fileslist, lines=True, chunksize=10000)
	# init total dict
	file_total_dict = initialize_total_dict(top100_list)
	for df in reddit_dfs:
		add_datetime(df)
		new_dict = fortnight_subreddit_totals(df, top100_list)
		# dicts are different between rows and between dicts here we are getting new dicts
		#print(new_dict) 
		file_total_dict['total'] = list( map(add, file_total_dict['total'], new_dict['total']) )
		for subreddit in subslist:
			file_total_dict[subreddit] = list( map(add, file_total_dict[subreddit], new_dict[subreddit]) )
		#file_total_dict = sum_totals(new_dict, file_total_dict, top100_list)
	return file_total_dict

# testing
def tester(dfs):
	for df in dfs:
		add_datetime(df)
		new_dict = fortnight_subreddit_totals(df, top100_list)
		return new_dict

def second_round_combo(resultslist):
	file_total_dict = initialize_total_dict(top100_list)
	for result_dict in resultslist:
		file_total_dict['total'] = list( map(add, file_total_dict['total'], result_dict['total']) )
		for subreddit in subslist:
			file_total_dict[subreddit] = list( map(add, file_total_dict[subreddit], result_dict[subreddit]) )
	return file_total_dict

def get_author_counts(filename):
	reddit_dfs = pd.read_json(filename, lines=True, chunksize=10000)
	subs_authors_dict = {}
	for df in reddit_dfs:
		new_sub_authors_dict = {}
		grouped_df = df.groupby('subreddit_id')
		# loop through grouped obj and make dict of sub ids and set
		for name, group in grouped_df:
			authors = set(group['author'].unique())
			if name in subs_authors_dict.keys():
				subs_authors_dict[name] = subs_authors_dict[name] | authors
			else:
				subs_authors_dict[name] = authors
	return subs_authors_dict

# TODO
def authors_subs_counts(filename,subslist):
	reddit_dfs = pd.read_json(filename, lines=True, chunksize=10000)
	columns_list = ['subreddit_id','week','comment_count','author_set']
	subs_authors_df = pd.DataFrame(data=None, columns=columns_list) # empty df to return later
	for df in reddit_dfs:
		for sub in subslist:
			new_sub_authors_dict = {}
			filtered_df = df[df['subreddit_id'] == sub]
			filtered_df = add_datetime(filtered_df)
			filtered_df = add_week(filtered_df)
			grouped_df = filtered_df.groupby('week')
			for name, group in grouped_df:
				# should set the number of the week
				this_week = name
				# should get a set of authors
				authors = set(group['author'].unique())
				# should get the number of comments, can add them later
				comment_count = len(group)
				# i think the first three values and sub id is already set
				# I think it's good so far I dunno what the next part does
				if name in subs_authors_dict.keys():
					subs_authors_dict[name] = subs_authors_dict[name] | authors
				else:
					subs_authors_dict[name] = authors
	return subs_authors_dict

def merge_set_dicts(setdictlist):
	"""
	takes a list of dicts. for each dict, the value is a set
	merges them into one dict where the value for each key is a set
	"""
	merged_dict = {}
	for onesetdict in setdictlist:
		for key, value in onesetdict.items():
			if key in merged_dict.keys():
				merged_dict[key] = merged_dict[key] | value
			else:
				merged_dict[key] = value
	return merged_dict

def dict_to_df(somedict):
	"""converts a dict with subreddit names for keys and sets of authors for values
	makes that into a pd dataframe of two columns
	"""
	name_list = []
	counts_list = []
	for key, value in somedict.items():
		name_list.append(key)
		counts_list.append(len(value))
	data = {'subreddit_id': name_list, 'author_counts': counts_list}
	new_df = pd.DataFrame(data=data)
	return new_df