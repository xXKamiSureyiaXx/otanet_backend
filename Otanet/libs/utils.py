import os
import re
import string

class Utils:
    def normalize_database_text(self, text):
        pattern = r'[^a-zA-Z0-9\s' + re.escape(string.punctuation) + ']'
        text = re.sub(pattern, '', text)
        return text
    
    def normalize_s3_text(self, text):
        text = text.get_title().lower().strip()
        text = re.sub(r"[^a-z0-9 ]", "", text)
        text = re.sub(r"\s+", "-", text)
        return text
    
    def is_float(self, input):
        try:
            float(input)
            return True
        except:
            return False
    
    def get_first_number(self, input):
                match = re.search(r'\d+', input)
                if match:
                    return int(match.group(0))
                return 0
    
    def create_tmp_dir(self, path):
        os.makedirs('tmp', exist_ok=True) 
        os.chdir("/tmp/")
        os.makedirs(path, exist_ok=True)