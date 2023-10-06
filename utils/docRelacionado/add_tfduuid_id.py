


def add_tfduuid_id(tfduuid, extracted_dicts):
    dicts_with_id = []
    for ind, dict_ in enumerate(extracted_dicts):
        if isinstance(dict_, dict):
            dicts_with_id.append({"tfduuid": tfduuid, "id": ind+1, **dict_})  
    return dicts_with_id
