import sys
def processFile(fileName):
	print 'processing file ', fileName
	fout=open('cleaned'+fileName,'w')
	with open(fileName) as f:
		for line in f:
			fout.write(line.translate(None,'()'))

	fout.close()
	
			

if __name__=='__main__':
	if len(sys.argv)!=2:
		print ' FileName Required as Input Argument'
		sys.exit(-1)

	processFile(sys.argv[1])

