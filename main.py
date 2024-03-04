import sys 
import utils

from IntuitiveAssociations import IADatabase, IAReporter  


if len(sys.argv) < 2: 
	print("Please specify a folder")
	sys.exit()

file_path = utils.validate_folder(str(sys.argv[1])) 
if not file_path:
	print("File not found")
	sys.exit()
if not utils.ensure_file_is_excel(file_path):
	print("File is not Excel")
	sys.exit()

print("Working with this file: " + file_path)

reporter = IAReporter().BuildJobReport(file_path)


