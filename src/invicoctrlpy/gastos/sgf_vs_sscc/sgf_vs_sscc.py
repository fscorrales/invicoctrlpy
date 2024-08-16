#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SGF vs SSCC
Data required:
    - SGF Resumen de Rendiciones por Proveedor
    - SSCC Consulta General de Movimientos
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
"""

import datetime as dt
from dataclasses import dataclass

import pandas as pd
from ...utils.import_dataframe import ImportDataFrame
# from invicodb.update import update_db


@dataclass
# --------------------------------------------------
class SGFVsSSCC(ImportDataFrame):
    ejercicio:str = str(dt.datetime.now().year)
    input_path:str = None
    db_path:str = None
    # update_db:bool = False

    # --------------------------------------------------
    def __post_init__(self):
        if self.db_path == None:
            self.get_db_path()
        # if self.update_db:
        #     self.update_sql_db()
        self.import_dfs()

    # --------------------------------------------------
    # def update_sql_db(self):
    #     if self.input_path == None:
    #         update_path_input = self.get_update_path_input()
    #     else:
    #         update_path_input = self.input_path

    #     update_sgf = update_db.UpdateSGF(
    #         update_path_input + '/Sistema Gestion Financiera', 
    #         self.db_path + '/sgf.sqlite')
    #     update_sgf.update_resumen_rend_prov()
        
    #     update_sscc = update_db.UpdateSSCC(
    #         update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
    #         self.db_path + '/sscc.sqlite')
    #     update_sscc.update_banco_invico()
    #     update_sscc.update_ctas_ctes()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.import_banco_invico()
        self.import_resumen_rend(self.ejercicio)

    # --------------------------------------------------
    def import_banco_invico(self):
        df = super().import_banco_invico(self.ejercicio)
        df = df.loc[df['movimiento'] != 'DEPOSITO']
        df['importe'] = df['importe'] * (-1)
        df.reset_index(drop=True, inplace=True)
        self.sscc_banco_invico = df
        return self.sscc_banco_invico

    # --------------------------------------------------
    def control_mes_cta_cte(self):
        sscc_mes_cta_cte = self.sscc_banco_invico.copy()
        sscc_mes_cta_cte = sscc_mes_cta_cte[(sscc_mes_cta_cte["cod_imputacion"] != "031")]
        sscc_mes_cta_cte = sscc_mes_cta_cte[['mes', 'cta_cte', 'importe']]
        sscc_mes_cta_cte = sscc_mes_cta_cte.groupby(['mes', 'cta_cte'], as_index=False).agg({'importe': 'sum'})
        sscc_mes_cta_cte.rename(columns={'importe': 'debitos_sscc'}, inplace=True)
        # sscc_mes_cta_cte = sscc_mes_cta_cte >> \
        #     dplyr.filter_(f.cod_imputacion != '031') >> \
        #     dplyr.select(f.mes, f.cta_cte, f.importe) >> \
        #     dplyr.group_by(f.mes, f.cta_cte) >> \
        #     dplyr.summarise(debitos_sscc = base.sum_(f.importe),
        #                     _groups = 'drop')
        sgf_mes_cta_cte = self.sgf_resumen_rend.copy()
        sgf_mes_cta_cte = sgf_mes_cta_cte.groupby(['mes', 'cta_cte'])[['importe_neto', 'retenciones']].sum().reset_index()
        sgf_mes_cta_cte.rename(columns={
            'importe_neto': 'neto_sgf', 'retenciones': 'retenciones_sgf'
        }, inplace=True)
        # sgf_mes_cta_cte = sgf_mes_cta_cte >> \
        #     dplyr.select(
        #         f.mes, f.cta_cte, 
        #         f.importe_neto, f.retenciones
        #     ) >> \
        #     dplyr.group_by(f.mes, f.cta_cte) >> \
        #     dplyr.summarise(
        #         neto_sgf = base.sum_(f.importe_neto),
        #         retenciones_sgf = base.sum_(f.retenciones),
        #         _groups = 'drop')
        control_mes_cta_cte = pd.merge(
            sgf_mes_cta_cte, sscc_mes_cta_cte, 
            on=['mes', 'cta_cte'], how='outer'
        )
        control_mes_cta_cte.fillna(0, inplace=True)
        control_mes_cta_cte['diferencia'] =(
            control_mes_cta_cte['neto_sgf'] + control_mes_cta_cte['retenciones_sgf'] - 
            control_mes_cta_cte['debitos_sscc']
        )
        control_mes_cta_cte = control_mes_cta_cte[
            ~((control_mes_cta_cte['diferencia'] > -0.1) & 
            (control_mes_cta_cte['diferencia'] < 0.1))
        ]
        # control_mes_cta_cte = sgf_mes_cta_cte >> \
        #     dplyr.full_join(sscc_mes_cta_cte) >> \
        #     dplyr.mutate(
        #         dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
        #     ) >> \
        #     dplyr.mutate(
        #         diferencia = f.neto_sgf + f.retenciones_sgf - f.debitos_sscc
        #     ) >> \
        #     dplyr.filter_(~dplyr.near(f.diferencia, 0))
        control_mes_cta_cte.sort_values(by=['mes', 'cta_cte'], inplace= True)
        control_mes_cta_cte = pd.DataFrame(control_mes_cta_cte)
        control_mes_cta_cte.reset_index(drop=True, inplace=True)
        return control_mes_cta_cte