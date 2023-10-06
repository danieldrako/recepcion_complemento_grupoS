import re

query = """
SELECT
tfdUUID,
maptostring(complemento) as 'ComplementoPagos'
FROM DocumentDB.FacturaPersistida fp 
WHERE "_TipoDeComprobante" = 'P'
LIMIT 20000;
"""
cols_=['tfdUUID', 'ComplementoPagos']

output_file = r"temp/para_pago.csv"

test_key = "_MonedaP"

concat_dict = {"uuid": "string",
               "FechaPago": "string", 
               "FormaDePagoP": 0, "MonedaP": "string", 
               "TipoCambioP": "string", 
               "Monto": 0, "NumOperacion": "string",
               "RfcEmisorCtaOrd": "string",
   "NomBancoOrdExt": "string",
   "CtaOrdenante": "string",
   "RfcEmisorCtaBen": "string",
   "CtaBeneficiario": "string",
   "TipoCadPago": "string",
   "CertPago": "string",
   "CadPago": "string",
   "SelloPago": "string"}

str_cols = ["uuid","FechaPago","MonedaP","TipoCambioP","NumOperacion",
                    "RfcEmisorCtaOrd","NomBancoOrdExt","CtaOrdenante","RfcEmisorCtaBen",
                    "CtaBeneficiario","TipoCadPago","CertPago","CadPago","SelloPago"]

float_cols = ["Monto"]

int_cols = ["FormaDePagoP"]

ordered_cols = ["uuid","FechaPago","FormaDePagoP","MonedaP","TipoCambioP","Monto",
                "NumOperacion","RfcEmisorCtaOrd", "NomBancoOrdExt","CtaOrdenante",
                "RfcEmisorCtaBen","CtaBeneficiario","TipoCadPago","CertPago","CadPago","SelloPago"]

new_name_tfduuid = "uuid"

to_delete = "uuid"

regular_exp = r":Pago" #sirve para saber que este elemento esté dentro del json principal

csv_file_path_to_post = "pago"

table_name = "test_pago"  # Nombre de la tabla en Vertica

schema = "DEV_FACTURACION"  # Esquema en Vertica