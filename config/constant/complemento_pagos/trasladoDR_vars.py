import re

query = """
SELECT
tfdUUID,
maptostring(complemento) as 'ComplementoPagos'
FROM DocumentDB.FacturaPersistida fp 
WHERE "_TipoDeComprobante" = 'P'
LIMIT 10000;
"""
cols_=['tfdUUID', 'ComplementoPagos']

output_file = r"temp/from_vertica_trasladoDR.csv"

test_key_one = "_IdDocumento"

test_key_two = "_BaseDR"

concat_dict = {
    "IdDocumento": "string",
    "ImpuestoDR": "string",
    "TasaOCuotaDR": 0,
    "BaseDR": 0,
    "TipoFactorDR": "string",
    "ImporteDR":0
}
str_cols = ['IdDocumento', "ImpuestoDR", "TipoFactorDR"]

float_cols = ["TasaOCuotaDR", "BaseDR", "ImporteDR"]

int_cols = []

ordered_cols = ["IdDocumento","BaseDR","ImpuestoDR","TipoFactorDR", "TasaOCuotaDR","ImporteDR"]

new_name_tfduuid = ""

to_delete = "IdDocumento"


regular_exp = r'TrasladoDR'

csv_file_path_to_post = "trasladoDR"

table_name = "new_trasladoDR"  # Nombre de la tabla en Vertica

schema = "DEV_FACTURACION"  # Esquema en Vertica