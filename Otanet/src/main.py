import os
import sys
path = os.getcwd()
parent_dir = os.path.abspath(os.path.join(path, os.pardir))

###### Import Player Object ######
sys.path.insert(0, f'{parent_dir}//libs')
from client_factory import MangaDexClient
##################################


client = MangaDexClient()
client.set_manga_list()