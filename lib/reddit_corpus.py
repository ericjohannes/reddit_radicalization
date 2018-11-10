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
	"""
	# should open subreddits file and create new file we append to
	if filetype is '.xz':
		f = lzma.open(inputfile, mode='rt')
	else:
		f = open(inputfile, 'r')
	
	newf = open(outputfile, 'a+')

	# loop through comments file, find comments containing subreddit_id and write that line to a file
	for line in f:
	    if subreddit_id in line:
	        newf.write(line)

	# be sure to close the files
	f.close()
	newf.close()
