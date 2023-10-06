import pandas as pd
from tqdm import tqdm
from pandas import json_normalize
tqdm.pandas()

def re_build_df(df,merge_dict,old_names,new_names, ordered_cols, num_cols, str_cols, int_cols):
    
    merge_df = pd.DataFrame(merge_dict)
    #df = pd.merge(merge_df,df, how="outer")
    df = pd.concat([merge_df,df])
    df = df.drop(df.index[0])
    
    for j, old_name in enumerate(old_names):
        new_name = new_names[j]
        df = df.rename(columns={old_name: new_name})
        
    for colu in df.columns:
        if colu in num_cols:
            df[colu] = df[colu].astype(float)
        elif  colu in str_cols:  
            df[colu] = df[colu].astype(str)
        elif colu in int_cols:
            df[colu] = df[colu].astype(float)
            df[colu] = df[colu].astype(int)    
            
    new_df = df[ordered_cols]
    return new_df