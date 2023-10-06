import re

query = """
SELECT
tfdUUID,
maptostring(complemento) as 'ComplementoPagos'
FROM DocumentDB.FacturaPersistida fp 
WHERE "_TipoDeComprobante" = 'P'
LIMIT 100;
"""
cols_=['tfdUUID', 'ComplementoPagos']

output_file = r"temp/para_totales.csv"

test_key = "_MontoTotalPagos"



str_cols = ['uuid']

float_cols = ['TotalRetencionesIVA','TotalRetencionesISR','TotalRetencionesIEPS','TotalTrasladosBaseIVA16',
    'TotalTrasladosImpuestoIVA16','TotalTrasladosBaseIVA8','TotalTrasladosImpuestoIVA8','TotalTrasladosBaseIVA0',
    'TotalTrasladosImpuestoIVA0','TotalTrasladosBaseIVAExento','MontoTotalPagos']

int_cols = ["FormaDePagoP"]

ordered_cols = ['uuid','TotalRetencionesIVA','TotalRetencionesISR','TotalRetencionesIEPS','TotalTrasladosBaseIVA16',
    'TotalTrasladosImpuestoIVA16','TotalTrasladosBaseIVA8','TotalTrasladosImpuestoIVA8','TotalTrasladosBaseIVA0',
    'TotalTrasladosImpuestoIVA0','TotalTrasladosBaseIVAExento','MontoTotalPagos']

new_name_tfduuid = "uuid"

to_delete = "uuid"

regular_exp = r":Totales" #sirve para saber que este elemento esté dentro del json principal

csv_file_path_to_post = "totales"

table_name = "new_totales"  # Nombre de la tabla en Vertica

schema = "DEV_FACTURACION"  # Esquema en Vertica