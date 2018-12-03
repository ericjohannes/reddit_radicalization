# should open subreddits file and create new file
f = open('../data/subreddits_basic.csv', 'r')
newf = open('../data/subreddits.csv', 'w+')

# loop through subreddits file, find The_Donald and write that line ot a file
for line in f:
    if 'The_Donald' in line:
        newf.write(line)
    if 'esist' in line:
    	newf.write(line)
    if 'Republican' in line:
    	newf.write(line)
    if 'Politics' in line:
    	newf.write(line)

# be sure to close the files
f.close()
newf.close()