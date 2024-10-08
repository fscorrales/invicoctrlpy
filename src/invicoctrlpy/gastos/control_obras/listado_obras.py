#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SGF vs SSCC
Data required:
    - Icaro
    - SGO Listado de Obras
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
import os
from dataclasses import dataclass

import pandas as pd
import numpy as np
from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicodb.update import update_db


@dataclass
# --------------------------------------------------
class ListadoObras(ImportDataFrame):
    input_path:str = None
    db_path:str = None
    update_db:bool = False

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

        update_icaro = update_db.UpdateIcaro(
            os.path.dirname(os.path.dirname(self.db_path)) + 
            '/R Output/SQLite Files/ICARO.sqlite', 
            self.db_path + '/icaro.sqlite')
        update_icaro.migrate_icaro()

        update_sgo = update_db.UpdateSGO(
            update_path_input + '/Sistema Gestion Obras', 
            self.db_path + '/sgo.sqlite')
        update_sgo.update_listado_obras()
        

    # --------------------------------------------------
    def import_dfs(self):
        pass

    def importIcaroObras(self) -> pd.DataFrame:
        df = super().import_icaro_obras()
        # df['obra']
        df = df.drop(columns=['id'])
        # Supongamos que tienes un DataFrame df con una columna 'columna_con_numeros' que contiene los registros con la parte numérica al principio
        df['obra'] = df['obra'].str.replace(r'^\d+-\d+', '', regex=True)
        df['obra'] = df['obra'].str.lstrip()
        df['imputacion'] = df['actividad'] + "-"+ df['partida']
        df = pd.concat(
            [df[['obra', 'imputacion']], 
            df.drop(columns=['obra', 'imputacion'])], axis=1
        )
        df['obra'] = df['obra'].str.slice(0, 85)
        df['obra'] = df['obra'].str.strip()

        return df
    
    def importSGOListadoObras(self) -> pd.DataFrame:
        df = super().import_sgo_listado_obras()
        df['obra'] = df['obra'].str.slice(0, 85)
        df['obra'] = df['obra'].str.strip()

        return df

    def icaroObrasConCodObras(self) -> pd.DataFrame:
        icaro = self.importIcaroObras().copy()
        # "CONSTRUCCION 2 VIV. Y OBRAS COMP. PROP. DEL CONJUNTO - MERCEDES" denominación incompleta en SGF
        # print(icaro.loc[icaro['obra'] =='PROG. LOTE PROPIO - SR. SANCHEZ GUILLERMO FRANCISCO']['obra'])
        sgo = self.importSGOListadoObras().copy()
        sgo = sgo.loc[:, ['cod_obra', 'obra']]
        # print(sgo.loc[sgo['obra'] =='PROG. LOTE PROPIO - SR. SANCHEZ GUILLERMO FRANCISCO']['obra'])
        df = icaro.merge(sgo, on='obra', how='left')
        df = df[['cod_obra'] + [col for col in df.columns if col != 'cod_obra']]
        df['cod_obra'] = np.where(
            (df['imputacion'].str.startswith('29-') & df['obra'].str[:3].str.isnumeric()), 
            df['obra'].str[:3],
            df['cod_obra']
        )
        return df
    
    def sgoObrasConImputacion(self) -> pd.DataFrame:
        icaro = self.importIcaroObras().copy()
        icaro = icaro.loc[:, ['imputacion', 'obra']]
        sgo = self.importSGOListadoObras().copy()
        df = sgo.merge(icaro, on='obra', how='left')
        df = df[['imputacion'] + [col for col in df.columns if col != 'imputacion']]
        return df
