#subreddit_id = 't5_38unr' # for r/The_Donald

subreddit_id = 't5_2cneq' # r/Politics
filename = '/l/research/social-media-mining/public/RC_2015-01-random-sample-1000000.jsonlines'

# should open subreddits file and create new file we append to
f = open(filename, 'r')
# newf = open('data/The_Donald_comments.json', 'a+')
newf = open('data/politics.json', 'a+')


# loop through comments file, find comments containing subreddit_id and write that line to a file
for line in f:
   if subreddit_id in line:
       newf.write(line)

# be sure to close the files
f.close()
newf.close()

