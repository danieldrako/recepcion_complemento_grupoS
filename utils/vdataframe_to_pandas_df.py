import os
# Obtiene la ruta del directorio actual donde se encuentra el script
ruta_script = os.path.abspath(__file__)

from concurrent.futures import ThreadPoolExecutor
import verticapy as vp
from verticapy import vDataFrame
from verticapy.utilities import *
import pandas as pd

def vdataframe_to_pandas_df(vdf):
    try:
        # Definir una función para convertir un vDataFrame a Pandas DataFrame
        def convert_to_pandas(vdf):
            return vdf.to_pandas()
        
        # Crear un ThreadPoolExecutor con un número deseado de hilos
        num_threads = 4  # Puedes ajustar este número según tus necesidades
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Dividir el vDataFrame en particiones para procesar en paralelo
            partitions = vdf.split()
            
            # Utilizar map para aplicar la función a cada partición en paralelo
            pd_dfs = list(executor.map(convert_to_pandas, partitions))
            
        # Concatenar los DataFrames de Pandas resultantes
        final_pd_df = pd.concat(pd_dfs, ignore_index=True)
        
        return final_pd_df
    
    except Exception as e:
        print("ERROR")
        print("Check: ", ruta_script)
        print("Error: ", e)
        
        
        
