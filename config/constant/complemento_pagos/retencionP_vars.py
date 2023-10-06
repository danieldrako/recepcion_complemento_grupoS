import re

query = """
SELECT
tfdUUID,
maptostring(complemento) as 'ComplementoPagos'
FROM DocumentDB.FacturaPersistida fp 
WHERE "_TipoDeComprobante" = 'P'
LIMIT 3000;
"""
cols_=['tfdUUID', 'ComplementoPagos']

output_file = r"temp/para_retencionp.csv"

test_key_one = ["pago20:RetencionesP", "pago10:RetencionesP"]
test_key_two = ['_ImporteP']



str_cols = ["uuid","ImpuestoP"]

float_cols = ["ImporteP"]

int_cols = []

ordered_cols = ["uuid","ImpuestoP","ImporteP"]

new_name_tfduuid = "uuid"

to_delete = "uuid"

regular_exp = r":RetencionesP" #sirve para saber que este elemento esté dentro del json principal

csv_file_path_to_post = "temp/retencionp"

table_name = "new_retencionP"  # Nombre de la tabla en Vertica

schema = "DEV_FACTURACION"  # Esquema en Vertica