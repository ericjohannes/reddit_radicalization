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
		with lzma.open(inputfile, mode='rt') as infile, open(outputfile, 'a+') as outfile:
			for line in infile:
				if subreddit_id in line:
					outfile.write(line)
	else:
		with open(inputfile, 'r') as infile, open(outputfile, 'a+') as outfile:
			for line in infile:
				if subreddit_id in line:
					outfile.write(line)
