import os

def validate_folder(folder):
    if os.path.exists(folder):
        return os.path.abspath(folder)
    else:
        return False
		
def ensure_file_is_excel(file_path):
    return (file_path.endswith(".xlsx") or file_path.endswith(".xls"))

def get_list_of_excel_files(folder): 
	return [f for f in os.listdir(folder) if (f.endswith(".xlsx") or f.endswith(".xls"))]

