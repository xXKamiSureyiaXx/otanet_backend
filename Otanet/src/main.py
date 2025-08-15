import os
import sys
path = os.getcwd()
parent_dir = os.path.abspath(os.path.join(path, os.pardir))

###### Import Player Object ######
sys.path.insert(0, f'{parent_dir}//libs')
from client_factory import MangaDexClient
from manga_factory import MangaFactory
from rds_helper import RDSHelper
##################################


client = MangaDexClient()
rds_helper = RDSHelper()

manga_objs = []
manga_list = client.get_manga_list()
rds_helper.create_table("manga_metadata")
for manga in manga_list:
    manga_obj = MangaFactory(manga)
    rds_helper.insert_manga_metadata("manga_metadata", manga_obj)
    manga_obj.download_manga()