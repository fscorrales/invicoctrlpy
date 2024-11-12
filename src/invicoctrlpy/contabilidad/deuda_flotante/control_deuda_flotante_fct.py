#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control Deuda Flontante (balance)
Data required:
    - SIIF rcg01_uejp
    - SIIF gto_rpa03g
    - SIIF rcocc31 (2122-1-2)
    - SIIF rdeu
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
import pandas as pd

from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicoctrlpy.utils import handle_path
from pydantic import BaseModel


class ControlDeudaFlotante(BaseModel):
    ejercicio:str
    db_path:str   = None
    rcocc31:pd.DataFrame = pd.DataFrame()
    rdeu012:pd.DataFrame = pd.DataFrame()
    rdeu_cta_contable:pd.DataFrame = pd.DataFrame()


def rdeu012_with_accounting(ejercicio:str = None, db_path:str = None) -> ControlDeudaFlotante:
    if not ejercicio:
        ejercicio = str(dt.datetime.now().year)
    if not db_path:
        db_path = handle_path.get_db_path()
    ctrl_rdeu = ControlDeudaFlotante(ejercicio=ejercicio, db_path=db_path)
    ctrl_rdeu.rcocc31 = rcocc31_in_rdue012(ejercicio=ejercicio, db_path=db_path)
    ctrl_rdeu.rdeu = import_siif_last_rdeu012(ejercicio=ejercicio, db_path=db_path)
    rdeu = ctrl_rdeu.rdeu.loc[
        :, ['ejercicio', 'fuente', 'cta_cte', 'nro_origen', 'saldo', 'cuit', 'glosa', 'nro_expte']
    ]
    rdeu = rdeu.rename(
        columns={'nro_origen': 'nro_original', 'saldo': 'saldo_rdeu'},
    )
    ctrl_rdeu.rdeu_cta_contable = rdeu.merge(
        ctrl_rdeu.rcocc31, how='left', on=['ejercicio', 'nro_original']
    )
    return ctrl_rdeu

def import_dataframes():
    import_df = ImportDataFrame()

    return import_df

# --------------------------------------------------
def import_siif_last_rdeu012(ejercicio:str, db_path:str) -> pd.DataFrame:
    import_df = ImportDataFrame()
    import_df.db_path = db_path
    df = import_df.import_siif_rdeu012(ejercicio=ejercicio)
    df = df.loc[df['mes_hasta'].str.endswith(ejercicio), :]
    months = df['mes_hasta'].tolist()
    # Convertir cada elemento de la lista a un objeto datetime
    dates = [dt.datetime.strptime(month, '%m/%Y') for month in months]
    # Obtener la fecha más reciente y Convertir la fecha mayor a un string en el formato 'MM/YYYY'
    gt_month = max(dates).strftime('%m/%Y')
    df = df.loc[df['mes_hasta'] == gt_month, :]
    return df
    # SELECT * FROM deuda_flotante_rdeu012
    # WHERE mes_hasta = '12/2023'

# --------------------------------------------------
def import_siif_rcocc31_liabilities(ejercicio:str, db_path:str):
    import_df = ImportDataFrame()
    import_df.db_path = db_path
    tipos_comprobantes = ['CAO', 'CAP', 'CAM', 'CAD', 'ANP']
    df = import_df.import_siif_rcocc31(ejercicio=ejercicio)
    df = df.loc[df['cta_contable'].str.startswith('2'), :]
    df = df.loc[df['tipo_comprobante'].isin(tipos_comprobantes), :]
    return df
    # SELECT sum(saldo) FROM mayor_contable_rcocc31
    # WHERE ejercicio = '2023'
    # AND tipo_comprobante <> 'CIE'
    # AND nro_original = '3369'

# --------------------------------------------------
def rcocc31_in_rdue012(ejercicio:str, db_path:str):
    import_df = ImportDataFrame()
    import_df.db_path = db_path
    rdeu = import_siif_last_rdeu012()
    rcocc31 = import_siif_rcocc31_liabilities()
    cyo = rdeu.loc[rdeu['ejercicio'] == ejercicio]['nro_comprobante'].tolist()
    cyo = list(map(lambda x: str(int(x[:-3])), cyo))
    df = rcocc31.loc[rcocc31['nro_original'].isin(cyo)]
    return df

if __name__ == '__main__':
    ctrl_rdeu = rdeu012_with_accounting(ejercicio='2023')
    print(ctrl_rdeu.rdeu_cta_contable)

# python -m invicoctrlpy.contabilidad.deuda_flotante.control_deuda_flotante