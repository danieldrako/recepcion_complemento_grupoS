import re

query = """
SELECT
tfdUUID,
maptostring(complemento) as 'ComplementoPagos'
FROM DocumentDB.FacturaPersistida fp 
WHERE "_TipoDeComprobante" = 'P'
LIMIT 30000;
"""
cols_=['tfdUUID', 'ComplementoPagos']

output_file = r"temp/para_trasladoP.csv"

test_key_one = ["pago20:TrasladosP", "pago10:TrasladosP"]
test_key_two = ['_BaseP']



str_cols = ["uuid","ImpuestoP","TipoFactorP"]

float_cols = ["BaseP","ImporteP","TasaOCuotaP"]

int_cols = []

ordered_cols = ["uuid","BaseP","ImpuestoP","TipoFactorP","TasaOCuotaP","ImporteP"]

new_name_tfduuid = "uuid"

to_delete = "uuid"

regular_exp = r":TrasladoP" #sirve para saber que este elemento esté dentro del json principal

csv_file_path_to_post = "trasladoP"

table_name = "new_trasladoP"  # Nombre de la tabla en Vertica

schema = "DEV_FACTURACION"  # Esquema en Vertica