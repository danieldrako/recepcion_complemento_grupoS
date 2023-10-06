

def add_tfduuid(tfduuid, extracted_dicts):
    dicts_with_id = []
    for ind, dict_ in enumerate(extracted_dicts):
        if isinstance(dict_, dict):
            dicts_with_id.append({"tfduuid": tfduuid,  **dict_})  #"id": ind+1,
    return dicts_with_id