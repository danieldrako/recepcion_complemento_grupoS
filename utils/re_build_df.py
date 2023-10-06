import os

# Obtiene la ruta del directorio actual donde se encuentra el script
ruta_script = os.path.abspath(__file__)
        
import pandas as pd


# def re_build_df(dataframe,concat_dict, ordered_cols, float_cols, 
#                 str_cols, int_cols, new_name_tfduuid="", to_delete=""):
#     try: 
#         df = dataframe.copy()  # Crear una copia del DataFrame original
        
#         for colu in dataframe.columns:
#             if colu.startswith('_'):
#                 new_col = colu.replace('_', '')
#                 df.rename(columns={colu: new_col}, inplace=True)
        
#         if new_name_tfduuid !="":
#             Buscar el índice de la columna que contiene "tfduuid" o "tfdUUID"
#             try:
#                 idx = df.columns.get_indexer(["tfduuid", "tfdUUID"])[0]
#                 El [0] en [0] se usa para obtener el primer índice encontrado
#             except IndexError:
#                 idx = None  # Si no se encuentra ninguna de las columnas, asignar None
#             Reemplazar el nombre de la columna en función de su índice
#             if idx is not None:
#                 df.columns.values[idx] = new_name_tfduuid
        
        
            
#         concat_df = pd.DataFrame([concat_dict])
#         df = pd.concat([concat_df,df])
        
#         for colu in df.columns:
#             if colu in float_cols:
#                 df[colu] = df[colu].astype(float)
#             elif  colu in str_cols:  
#                 df[colu] = df[colu].astype(str)
#             elif colu in int_cols:
#                 df[colu] = df[colu].astype(float)
#                 df[colu] = df[colu].astype(int)    
        
#         Reemplazar 'nan' con cadenas vacías y eliminar filas con 'string' en la columna df.columns.values[idx]
#         df = df.replace('nan', '').fillna('')
#         df = df[df[to_delete] != "string"]
                
#         df = df[ordered_cols]
#         return df
#     except Exception as e:
#         print("==============_ERROR_==============")
#         print("Check: ", ruta_script)
#         print("Error: ", e)
# def re_build_df(dataframe,concat_dict, ordered_cols, float_cols, 
#                 str_cols, int_cols, new_name_tfduuid="", to_delete=""):
 
#         df = dataframe.copy()  # Crear una copia del DataFrame original
        
#         for colu in dataframe.columns:
#             if colu.startswith('_'):
#                 new_col = colu.replace('_', '')
#                 df.rename(columns={colu: new_col}, inplace=True)
#         print(df.tail())
#         print(df.columns)
#         print("despues de quitar los guiones de las columnas")
#         print("#"*60)
#         if new_name_tfduuid !="":
#             # Buscar el índice de la columna que contiene "tfduuid" o "tfdUUID"
#             try:
#                 idx = df.columns.get_indexer(["tfduuid", "tfdUUID"])[0]
#                 # El [0] en [0] se usa para obtener el primer índice encontrado
#             except IndexError:
#                 idx = None  # Si no se encuentra ninguna de las columnas, asignar None
#             # Reemplazar el nombre de la columna en función de su índice
#             if idx is not None:
#                 df.columns.values[idx] = new_name_tfduuid
        
#         print(df.tail())
#         print(df.columns)
#         print("despues de renombrar tfduuid")
#         print("#"*60)
            
#         concat_df = pd.DataFrame([concat_dict])
#         df = pd.concat([df,concat_df])
#         print(df.tail())
#         print(df.columns)
#         print("despues de renombrar concatenar")
#         print("#"*60)
#         for colu in df.columns:
#             if colu in float_cols:
#                 df[colu] = df[colu].astype(float)
#             elif  colu in str_cols:  
#                 df[colu] = df[colu].astype(str)
#             elif colu in int_cols:
#                 df[colu] = df[colu].astype(float)
#                 df[colu] = df[colu].astype(int)    
#         print(df.tail())
#         print(df.columns)
#         print("despues de cambiar el tipo de variables")
#         print("#"*60)
#         # Reemplazar 'nan' con cadenas vacías y eliminar filas con 'string' en la columna df.columns.values[idx]
#         df = df.replace('nan', '').fillna('')
#         df = df[df[to_delete] != "string"]
#         print(df.tail())
#         print(df.columns)
#         print("despues de filrtrar los nan y eliminar todos los datos donde uuid era string")
#         print("#"*60)
                
#         df = df[ordered_cols]
#         return df



def re_build_df(dataframe, ordered_cols, float_cols, 
                str_cols, int_cols, new_name_tfduuid="", to_delete=""):
 
        df = dataframe.copy()  # Crear una copia del DataFrame original

        for colu in dataframe.columns:
            if colu.startswith('_'):
                new_col = colu.replace('_', '')
                df.rename(columns={colu: new_col}, inplace=True)
        
        # Renombrar la columna "tfduuid" o "tfdUUID" según el valor de new_name_tfduuid
        if "tfduuid" in df.columns:
            df.rename(columns={"tfduuid": new_name_tfduuid}, inplace=True)
        elif "tfdUUID" in df.columns:
            df.rename(columns={"tfdUUID": new_name_tfduuid}, inplace=True)
        
        # Agregar columnas faltantes con valores vacíos
        for new_col in ordered_cols:
            if new_col not in df.columns:
                df[new_col] = ""

        # Cambiar tipos de datos con manejo de excepciones
        for colu in df.columns:
            try:
                if colu in float_cols:
                    df[colu] = df[colu].astype(float)
                elif colu in str_cols:
                    df[colu] = df[colu].astype(str)
                elif colu in int_cols:
                    df[colu] = df[colu].astype(float).astype(int)
            except ValueError:
                print(f"Error de conversión en la columna {colu}  |Se mantuvo como tipo de dato original.")

        # Reemplazar 'nan' con cadenas vacías y eliminar filas según to_delete
        df = df.replace('nan', '').fillna('')

        # Filtrar columnas según ordered_cols
        df = df[ordered_cols]
        return df