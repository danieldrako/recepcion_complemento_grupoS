import re

query = """
SELECT
tfdUUID,
maptostring(Complemento) as ComplementoPagos
FROM PST_DES_REC.FACTURA f
WHERE 
tfdUUID 
NOT IN (SELECT DISTINCT nr.'uui' FROM DEV_FACTURACION.new_recpago nr)
AND f."_TipoDeComprobante" in('P') 
LIMIT 1500000;
"""
cols_=['tfdUUID', 'ComplementoPagos']

output_file = r"FROM_Vertica_Complemento_pago.csv"

tables_dest = {"pago":"new_recpago", "docRelacionado":"new_recdocRelacionado", "totales": "new_rectotales"
               ,"trasladoDR":"new_rectrasladoDR" , "trasladoP":"new_rectrasladoP"
               , "retencionDR": "new_recretencionDR", "retencionP":"new_recretencionP"}

schema = "DEV_FACTURACION"


