import re

query = """
SELECT
tfdUUID,
maptostring(complemento) as 'ComplementoPagos'
FROM DocumentDB.FacturaPersistida fp 
WHERE "_TipoDeComprobante" = 'P'
LIMIT 50;
"""
cols_=['tfdUUID', 'ComplementoPagos']

output_file = r"temp/para_docRelacionado.csv"

test_key = "_IdDocumento"

concat_dict ={
    "id":0, 'uuid': "string", 
    "IdDocumento":"string",
    "Serie":"--string--",
    "Folio":"--string--",
    "MonedaDR":"--string--",
    'EquivalenciaDR':0, "NumParcialidad":0, "ImpSaldoAnt":0, "ImpPagado":0, "ImpSaldoInsoluto":0, 'ObjetoImpDR':"--string--"
}
str_cols = ['uuid',  'IdDocumento', 'Serie', 'Folio', 'MonedaDR', 'ObjetoImpDR']

float_cols = ['EquivalenciaDR','NumParcialidad', 'ImpSaldoAnt', 'ImpPagado', 'ImpSaldoInsoluto']

int_cols = ["id"]

ordered_cols = ["id", 'uuid', "IdDocumento", "Serie", "Folio", "MonedaDR",
    'EquivalenciaDR', "NumParcialidad", "ImpSaldoAnt", "ImpPagado", "ImpSaldoInsoluto", 'ObjetoImpDR']

new_name_tfduuid = "uuid"

to_delete = "uuid"

regular_exp = r'DoctoRelacionado' #sirve para saber que este elemento esté dentro del json principal

csv_file_path_to_post = "docRelacionado"

table_name = "test_docRelacionado"  # Nombre de la tabla en Vertica

schema = "DEV_FACTURACION"  # Esquema en Vertica