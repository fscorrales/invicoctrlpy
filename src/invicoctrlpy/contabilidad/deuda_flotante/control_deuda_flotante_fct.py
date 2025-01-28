#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control Deuda Flontante (balance)
Data required:
    - SIIF rcocc31 (2000)
    - SIIF rdeu012
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

__all__ = ['rdeu012_with_accounting']

import datetime as dt

import pandas as pd
from invicoctrlpy.utils import handle_path
from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from pydantic import BaseModel, ConfigDict


# --------------------------------------------------
class ControlDeudaFlotante(BaseModel):
    ejercicios:list[str] = None
    rcocc31:pd.DataFrame = pd.DataFrame()
    rdeu012:pd.DataFrame = pd.DataFrame()
    rdeu_cta_contable:pd.DataFrame = pd.DataFrame()
    model_config = ConfigDict(arbitrary_types_allowed=True)


# --------------------------------------------------
def rdeu012_with_accounting(ejercicios:[str] = None, db_path:str = None) -> ControlDeudaFlotante:
    if not ejercicios:
        ejercicios = str(dt.datetime.now().year)
    if not isinstance(ejercicios, list):
        ejercicios = [ejercicios]
    if not db_path:
        db_path = handle_path.get_db_path()
    ejercicios = sorted([str(ejercicio) for ejercicio in ejercicios], key=lambda x: int(x))
    ctrl_rdeu = ControlDeudaFlotante(ejercicios=ejercicios)
    import_df = set_import_df_db_path(db_path=db_path)
    for ejercicio in ejercicios:
        rdeu = import_siif_last_rdeu012(ejercicio=ejercicio, import_df=import_df)
        rcocc31 = import_siif_rcocc31_liabilities(ejercicio=ejercicio, import_df=import_df)
        filter_rcocc31 = rcocc31_in_rdue012(ejercicio=ejercicio, rdeu=rdeu, rcocc31=rcocc31)
        rdeu['ejercicio_contable'] = ejercicio
        rdeu = rdeu[['ejercicio_contable'] + [col for col in rdeu.columns if col != 'ejercicio_contable']]
        ctrl_rdeu.rdeu012 = pd.concat(
            [ctrl_rdeu.rdeu012, rdeu]
        )
        ctrl_rdeu.rcocc31 = pd.concat(
            [ctrl_rdeu.rcocc31, filter_rcocc31]
        )
        rdeu = rdeu.loc[
            :, [
                'ejercicio_contable','ejercicio', 'fuente', 'cta_cte', 'nro_original', 
                'saldo_rdeu', 'cuit', 'glosa', 'nro_expte'
            ]
        ]
        ctrl_rdeu.rdeu_cta_contable = pd.concat(
            [
                ctrl_rdeu.rdeu_cta_contable, 
                rdeu.merge(
                    ctrl_rdeu.rcocc31, how='left', on=['ejercicio', 'nro_original']
                )
            ]
        )
        aju = aju_not_in_rdue012(filter_rdeu=rdeu, rcocc31=ctrl_rdeu.rcocc31, ejercicio=ejercicio)
        ctrl_rdeu.rdeu_cta_contable = pd.concat([ctrl_rdeu.rdeu_cta_contable, aju])

    return ctrl_rdeu


# --------------------------------------------------
def set_import_df_db_path(db_path:str) -> ImportDataFrame:
    import_df = ImportDataFrame()
    import_df.db_path = db_path
    return import_df


# --------------------------------------------------
def import_siif_last_rdeu012(ejercicio:str, import_df:ImportDataFrame) -> pd.DataFrame:
    df = import_df.import_siif_rdeu012()
    df = df.loc[df['mes_hasta'].str.endswith(ejercicio), :]
    months = df['mes_hasta'].tolist()
    # Convertir cada elemento de la lista a un objeto datetime
    dates = [dt.datetime.strptime(month, '%m/%Y') for month in months]
    # Obtener la fecha más reciente y Convertir la fecha mayor a un string en el formato 'MM/YYYY'
    gt_month = max(dates).strftime('%m/%Y')
    df = df.loc[df['mes_hasta'] == gt_month, :]
    df = df.rename(
        columns={'nro_origen': 'nro_original', 'saldo': 'saldo_rdeu'},
    )
    return df


# --------------------------------------------------
def import_siif_rcocc31_liabilities(ejercicio:str, import_df:ImportDataFrame):
    tipos_comprobantes = ['CAO', 'CAP', 'CAM', 'CAD', 'ANP', 'AJU']
    df = import_df.import_siif_rcocc31(ejercicio=ejercicio)
    df = df.loc[df['cta_contable'].str.startswith('2'), :]
    df = df.loc[df['tipo_comprobante'].isin(tipos_comprobantes), :]
    df = df.rename(columns={'saldo': 'saldo_contable'})
    return df

# --------------------------------------------------
def rcocc31_in_rdue012(rdeu:pd.DataFrame, rcocc31:pd.DataFrame, ejercicio:str) -> pd.DataFrame:
    rdeu = rdeu.loc[rdeu['ejercicio'] == ejercicio, :]
    cyo = rdeu.loc[rdeu['ejercicio'] == ejercicio]['nro_comprobante'].tolist()
    cyo = list(map(lambda x: str(int(x[:-3])), cyo))
    aju = rcocc31.loc[rcocc31['tipo_comprobante'] == 'AJU']
    df = rcocc31.loc[rcocc31['nro_original'].isin(cyo)]
    df = pd.concat([df, aju])
    return df


# --------------------------------------------------
def aju_not_in_rdue012(filter_rdeu:pd.DataFrame, rcocc31:pd.DataFrame, ejercicio:str) -> pd.DataFrame:
    aju = rcocc31.loc[rcocc31['tipo_comprobante'] == 'AJU']
    aju['nro_comprobante'] = aju['nro_entrada'] + '/' + aju['ejercicio'].str[2:]
    # Elimino comprobantes específicos (error en SIIF)
    aju = aju.loc[~aju['nro_comprobante'].isin(['17097/13'])] # PAGO COMPR. CAO 5413/13. MAP 5429/13. ERROR SISTEMA
    # Conservo los AJU con saldo mayor a 0.1 y el 16536/11
    aju_keep = aju.loc[aju['nro_comprobante'].isin(['16536/11'])]
    aju_keep = aju_keep.drop(columns=['nro_comprobante'])
    filtered_aju = aju.groupby('nro_original').sum()['saldo_contable']
    filtered_aju = filtered_aju[abs(filtered_aju) > 0.1]
    aju = aju.merge(filtered_aju.reset_index()['nro_original'], on='nro_original', how='right')
    aju = pd.concat([aju, aju_keep], axis=0)
    df = pd.DataFrame(columns=filter_rdeu.columns)
    df = df.drop(columns=['ejercicio', 'nro_original'])
    df = pd.concat([df, aju], axis=1)
    df['ejercicio_contable'] = ejercicio
    df['fuente'] = '11'
    df['saldo_rdeu'] = df['saldo_contable'] * (-1)
    return df   


# --------------------------------------------------
if __name__ == '__main__':
    ejercicios = [str(x) for x in range(2010, 2025)]
    ctrl_rdeu = rdeu012_with_accounting(ejercicios=['2012'])
    print(ctrl_rdeu.rdeu_cta_contable)

# python -m invicoctrlpy.contabilidad.deuda_flotante.control_deuda_flotante_fct