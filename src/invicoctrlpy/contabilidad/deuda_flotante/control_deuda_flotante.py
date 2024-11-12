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

__all__ = ['ControlDeudaFlotante']

import datetime as dt
import pandas as pd
from dataclasses import dataclass, field

from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicoctrlpy.utils import handle_path


# --------------------------------------------------
@dataclass
class ControlDeudaFlotante():
    ejercicios:[str] = field(default_factory=lambda: [str(dt.datetime.now().year)])
    db_path:str = None
    import_df:ImportDataFrame = field(init=False, repr=False)

    # --------------------------------------------------
    def __post_init__(self):
        if self.db_path is None:
            self.db_path = handle_path.get_db_path()
        if not isinstance(self.ejercicios, list):
            self.ejercicios = [self.ejercicios]
        self.import_df = ImportDataFrame()
        self.import_df.db_path = self.db_path


    # --------------------------------------------------
    def import_siif_last_rdeu012(self, ejercicio:str = None):
        df = self.import_df.import_siif_rdeu012()
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
        # SELECT * FROM deuda_flotante_rdeu012
        # WHERE mes_hasta = '12/2023'

    # --------------------------------------------------
    def import_siif_rcocc31_liabilities(self, ejercicio:str = None):
        tipos_comprobantes = ['CAO', 'CAP', 'CAM', 'CAD', 'ANP']
        df = self.import_df.import_siif_rcocc31(ejercicio=ejercicio)
        df = df.loc[df['cta_contable'].str.startswith('2'), :]
        df = df.loc[df['tipo_comprobante'].isin(tipos_comprobantes), :]
        df = df.rename(
            columns={'saldo': 'saldo_contable'},
        )
        return df
        # SELECT sum(saldo) FROM mayor_contable_rcocc31
        # WHERE ejercicio = '2023'
        # AND tipo_comprobante <> 'CIE'
        # AND nro_original = '3369'

    # --------------------------------------------------
    def filter_rcocc31_in_rdue012(self):
        dfs = pd.DataFrame()
        for ejercicio in self.ejercicios:
            rdeu = self.import_siif_last_rdeu012(ejercicio)
            rdeu = rdeu.loc[rdeu['ejercicio'] == ejercicio, :]
            rcocc31 = self.import_siif_rcocc31_liabilities(ejercicio)
            cyo = rdeu.loc[rdeu['ejercicio'] == ejercicio]['nro_comprobante'].tolist()
            cyo = list(map(lambda x: str(int(x[:-3])), cyo))
            df = rcocc31.loc[rcocc31['nro_original'].isin(cyo)]
            dfs = pd.concat([df, dfs], ignore_index=True)
        return dfs

    # --------------------------------------------------
    def rdeu012_with_accounting(self):
        rcocc31 = self.filter_rcocc31_in_rdue012()
        rdeu = self.import_siif_last_rdeu012(max(self.ejercicios))
        rdeu = rdeu.loc[
            :, ['ejercicio', 'fuente', 'cta_cte', 'nro_original', 'saldo_rdeu', 'cuit', 'glosa', 'nro_expte']
        ]
        df = rdeu.merge(rcocc31, how='left', on=['ejercicio', 'nro_original'])
        return df

if __name__ == '__main__':
    ejercicios = [str(ejercicio) for ejercicio in range(2010, 2024)]
    siif = ControlDeudaFlotante(ejercicios=ejercicios)
    df = siif.rdeu012_with_accounting()
    print(df)
    print(df.columns)
    print(df.iloc[1])

# python -m invicoctrlpy.contabilidad.deuda_flotante.control_deuda_flotante