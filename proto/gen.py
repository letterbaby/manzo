import os
import re
import shutil

def main():
	basePath = "../"
	for file in os.listdir('.'):
		fileInfo = os.path.splitext(file)
		if len(fileInfo) == 2 and fileInfo[1].lower() == ".proto":
			genPath = basePath + fileInfo[0].lower()
			if not os.path.isdir(genPath) :
				os.mkdir(genPath)
			cmd = "protoc --go_out={0} {1}".format(genPath, file.lower())
			print(cmd)
			os.system(cmd)
	
if __name__ == '__main__':
	main()
