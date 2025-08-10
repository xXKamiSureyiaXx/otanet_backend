import os
import sys
path = os.getcwd()
parent_dir = os.path.abspath(os.path.join(path, os.pardir))

###### Import Player Object ######
sys.path.insert(0, f'{parent_dir}//libs')
from client_factory import MangaDexClient
from manga_factory import MangaFactory
##################################


client = MangaDexClient()
manga_list = client.get_manga_list()
manga_objs = []
for manga in manga_list:
    manga_obj = MangaFactory(manga)
    manga_objs.append(manga_obj)

for obj in manga_objs:
    obj.download_manga()
