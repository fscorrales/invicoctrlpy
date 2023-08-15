#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: Distintos informes relacionados a la ejecución de Obras
Data required:
    - Icaro
    - SIIF rf602
    - SIIF rf610
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
import os
from dataclasses import dataclass, field

import pandas as pd
from datar import base, dplyr, f, tidyr
from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicodb.update import update_db


@dataclass
# --------------------------------------------------
class EjecucionGastos(ImportDataFrame):
    ejercicio:str = str(dt.datetime.now().year)
    input_path:str = None
    db_path:str = None
    update_db:bool = False
    siif_desc_pres:pd.DataFrame = field(init=False, repr=False)
    siif_ejec_obras:pd.DataFrame = field(init=False, repr=False)

    # --------------------------------------------------
    def __post_init__(self):
        if self.db_path == None:
            self.get_db_path()
        if self.update_db:
            self.update_sql_db()
        self.import_dfs()

    # --------------------------------------------------
    def update_sql_db(self):
        if self.input_path == None:
            update_path_input = self.get_update_path_input()
        else:
            update_path_input = self.input_path

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_ppto_gtos_fte_rf602()
        update_siif.update_ppto_gtos_desc_rf610()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.siif_desc_pres = self.import_siif_desc_pres(ejercicio_to=self.ejercicio)

    # --------------------------------------------------
    def import_siif_gtos_desc(self):
        df = super().import_siif_rf602(self.ejercicio)
        #df = df.loc[df['ordenado'] > 0]
        df.sort_values(by=['ejercicio', 'estructura'], ascending=[False, True],inplace=True)
        df = df.merge(self.siif_desc_pres, how='left', on='estructura', copy=False)
        df.drop(
            labels=['org', 'pendiente', 
            'subprograma', 'proyecto', 'actividad'], 
            axis=1, inplace=True
            )
        df["programa"] = df['programa'].astype('int')
        df = df >>\
            dplyr.select(
                f.ejercicio, f.estructura, f.partida, f.fuente,
                f.desc_prog, f.desc_subprog, f.desc_proy, f.desc_act,
                dplyr.everything()
            )
        df = pd.DataFrame(df)
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_comprobantes(self) -> pd.DataFrame:
        return super().import_siif_comprobantes(self.ejercicio)