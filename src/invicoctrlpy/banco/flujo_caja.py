#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Cash Flow from INVICO's SSCC Consulta Gral. de Mov.
Data required:
    - SSCC Consulta General de Movimiento
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
from dataclasses import dataclass

import numpy as np
import pandas as pd
from invicodb.update import update_db
from datar import base, dplyr, f, tidyr

from invicoctrlpy.utils.import_dataframe import ImportDataFrame


@dataclass
# --------------------------------------------------
class FlujoCaja(ImportDataFrame):
    ejercicio:str = None
    input_path:str = None
    db_path:str = None
    update_db:bool = False

    # --------------------------------------------------
    def __post_init__(self):
        if self.db_path == None:
            self.get_db_path()
        if self.update_db:
            self.update_sql_db()
        if self.ejercicio == '':
            self.ejercicio = None
        self.import_dfs()

    # --------------------------------------------------
    def update_sql_db(self):
        if self.input_path == None:
            update_path_input = self.get_update_path_input()
        else:
            update_path_input = self.input_path
        
        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_banco_invico()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()

    # --------------------------------------------------
    def import_banco_invico(self):
        df = super().import_banco_invico(self.ejercicio)
        recursos = [
            '002', '001', '012', '211', '056', 
            '216', '218', '220', '222', '057'
        ]
        funcionamiento = [
            '023', '024', '029', '027', '031',
            '032', '033', '036', '037', '040',
            '043', '049', '059'
        ]
        inversion_obras = [
            '019', '020', '021', '027', '041',
            '052', '053', '065', '066', '072',
            '143', '210', '217', '219', '221',
            '142', '035', '213'
        ]
        # df['clase'] = np.where(df['cod_imputacion'].isin(recursos), 'Recursos',
        #                 np.where(df['cod_imputacion'].isin(funcionamiento), 'Gtos. Funcionamiento',
        #                     np.where(df['cod_imputacion'].isin(inversion_obras), 'Inversión Obras',  
        #                     'OTROS')))
        df['clase'] = np.select(
            [
                df['cod_imputacion'].isin(recursos),
                df['cod_imputacion'].isin(funcionamiento),
                df['cod_imputacion'].isin(inversion_obras),
            ], 
            ['1 - Recursos', '2 - Gtos. Funcionamiento', '3 - Inversión Obras'], 
            '4 - OTROS'
        )
        df.reset_index(drop=True, inplace=True)
        df = df.sort_values(by='clase')
        return df

    # --------------------------------------------------
    def flujo_caja_anual(self):
        sscc = self.import_banco_invico()
        groupby_cols:list = ['ejercicio', 'clase']
        sscc = sscc.groupby(groupby_cols)['importe'].sum()
        sscc = sscc.reset_index()
        return sscc


    # # --------------------------------------------------
    # def control_mes_grupo(self):
    #     siif_mes_gpo = self.import_siif_rci02()
    #     siif_mes_gpo = siif_mes_gpo.loc[siif_mes_gpo['es_invico'] == False]
    #     siif_mes_gpo = siif_mes_gpo.loc[siif_mes_gpo['es_remanente'] == False]
    #     siif_mes_gpo = siif_mes_gpo >> \
    #         dplyr.select(f.mes, f.grupo, f.importe) >> \
    #         dplyr.group_by(f.mes, f.grupo) >> \
    #         dplyr.summarise(recursos_siif = base.sum_(f.importe),
    #                         _groups = 'drop')
    #     sscc_mes_gpo = self.import_banco_invico()
    #     sscc_mes_gpo = sscc_mes_gpo >> \
    #         dplyr.select(
    #             f.mes, f.grupo, 
    #             f.importe
    #         ) >> \
    #         dplyr.group_by(f.mes, f.grupo) >> \
    #         dplyr.summarise(
    #             depositos_sscc = base.sum_(f.importe),
    #             _groups = 'drop')
    #     control_mes_gpo = siif_mes_gpo >> \
    #         dplyr.full_join(sscc_mes_gpo) >> \
    #         dplyr.mutate(
    #             dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
    #         ) >> \
    #         dplyr.mutate(
    #             diferencia = f.recursos_siif - f.depositos_sscc
    #         )
    #         # dplyr.filter_(~dplyr.near(f.diferencia, 0))
    #     control_mes_gpo.sort_values(by=['mes', 'grupo'], inplace= True)
    #     control_mes_gpo = pd.DataFrame(control_mes_gpo)
    #     control_mes_gpo.reset_index(drop=True, inplace=True)
    #     return control_mes_gpo

    # # --------------------------------------------------
    # def control_mes_grupo_cta_cte(self):
    #     siif_mes_gpo_cta_cte = self.import_siif_rci02()
    #     siif_mes_gpo_cta_cte = siif_mes_gpo_cta_cte.loc[siif_mes_gpo_cta_cte['es_invico'] == False]
    #     siif_mes_gpo_cta_cte = siif_mes_gpo_cta_cte.loc[siif_mes_gpo_cta_cte['es_remanente'] == False]
    #     siif_mes_gpo_cta_cte = siif_mes_gpo_cta_cte >> \
    #         dplyr.select(f.mes, f.grupo, f.cta_cte, f.importe) >> \
    #         dplyr.group_by(f.mes, f.grupo, f.cta_cte) >> \
    #         dplyr.summarise(recursos_siif = base.sum_(f.importe),
    #                         _groups = 'drop')
    #     sscc_mes_gpo_cta_cte = self.import_banco_invico()
    #     sscc_mes_gpo_cta_cte = sscc_mes_gpo_cta_cte >> \
    #         dplyr.select(
    #             f.mes, f.grupo, f.cta_cte,
    #             f.importe
    #         ) >> \
    #         dplyr.group_by(f.mes, f.grupo, f.cta_cte) >> \
    #         dplyr.summarise(
    #             depositos_sscc = base.sum_(f.importe),
    #             _groups = 'drop')
    #     control_mes_gpo_cta_cte = siif_mes_gpo_cta_cte >> \
    #         dplyr.full_join(sscc_mes_gpo_cta_cte) >> \
    #         dplyr.mutate(
    #             dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
    #         ) >> \
    #         dplyr.mutate(
    #             diferencia = f.recursos_siif - f.depositos_sscc
    #         )
    #         # dplyr.filter_(~dplyr.near(f.diferencia, 0))
    #     control_mes_gpo_cta_cte.sort_values(by=['mes', 'grupo', 'cta_cte'], inplace= True)
    #     control_mes_gpo_cta_cte = pd.DataFrame(control_mes_gpo_cta_cte)
    #     control_mes_gpo_cta_cte.reset_index(drop=True, inplace=True)
    #     return control_mes_gpo_cta_cte

    # # --------------------------------------------------
    # def control_recursos(self):
    #     group_by = ['ejercicio', 'mes', 'cta_cte', 'grupo']
    #     siif = self.import_siif_rci02()
    #     siif = siif.loc[siif['es_invico'] == False]
    #     siif = siif.loc[siif['es_remanente'] == False]
    #     siif = siif.groupby(group_by)['importe'].sum()
    #     siif = siif.reset_index(drop=False)
    #     siif = siif.rename(columns={'importe':'recursos_siif'})
    #     sscc = self.import_banco_invico()
    #     sscc = sscc.groupby(group_by)['importe'].sum()
    #     sscc = sscc.reset_index(drop=False)
    #     sscc = sscc.rename(columns={'importe':'depositos_banco'})
    #     control = pd.merge(siif, sscc, how='outer')       
    #     return control