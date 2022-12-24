#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: SGF vs SSCC
Data required:
    - SGF Resumen de Rendiciones por Proveedor
    - SSCC Consulta General de Movimientos
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
"""

import datetime as dt
import inspect
import os
from dataclasses import dataclass, field

import pandas as pd
from datar import base, dplyr, f, tidyr
from invicodatpy.sgf.resumen_rend_prov import ResumenRendProv
from invicodatpy.sscc.all import BancoINVICO, CtasCtes
import update_db


@dataclass
# --------------------------------------------------
class SGFVsSSCC():
    ejercicio:str = str(dt.datetime.now().year)
    db_path:str = None
    update_db:bool = False
    ctas_ctes:pd.DataFrame = field(init=False, repr=False)
    sgf_resumen_rend:pd.DataFrame = field(init=False, repr=False)
    sscc_banco_invico:pd.DataFrame = field(init=False, repr=False)

    # --------------------------------------------------
    def __post_init__(self):
        if self.db_path == None:
            self.get_db_path()
        if self.update_db:
            self.update_sql_db()
        self.import_dfs()

    # --------------------------------------------------
    def get_outside_path(self):
        dir_path = os.path.dirname(
                        os.path.dirname(
                            os.path.dirname(
                                os.path.dirname(
                                    os.path.dirname(
                                        os.path.abspath(
                                            inspect.getfile(
                                                inspect.currentframe()))
                        )))))
        return dir_path

    # --------------------------------------------------
    def get_db_path(self):
        self.db_path = (self.get_outside_path() 
                        + '/Python Output/SQLite Files')
        return self.db_path

    # --------------------------------------------------
    def get_update_path_input(self):
        dir_path = (self.get_outside_path() 
                    + '/invicoDB/Base de Datos')
        return dir_path

    # --------------------------------------------------
    def update_sql_db(self):
        update_path_input = self.get_update_path_input()

        update_sgf = update_db.UpdateSGF(
            update_path_input + '/Sistema Gestion Financiera', 
            self.db_path + '/sgf.sqlite')
        update_sgf.update_resumen_rend_prov()
        
        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_banco_invico()
        update_sscc.update_ctas_ctes()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.import_banco_invico()
        self.import_resumen_rend()

    # --------------------------------------------------
    def import_ctas_ctes(self):
        df = CtasCtes().from_sql(self.db_path + '/sscc.sqlite') 
        self.ctas_ctes = df
        return self.ctas_ctes

    # --------------------------------------------------
    def import_banco_invico(self):
        df = BancoINVICO().from_sql(self.db_path + '/sscc.sqlite')  
        df = df.loc[df['ejercicio'] == self.ejercicio]
        df = df.loc[df['movimiento'] != 'DEPOSITO']
        df['importe'] = df['importe'] * (-1)
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'sscc_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='sscc_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'sscc_cta_cte'], axis='columns', inplace=True)
        self.sscc_banco_invico = df
        return self.sscc_banco_invico

    # --------------------------------------------------
    def import_resumen_rend(self):
        df = ResumenRendProv().from_sql(self.db_path + '/sgf.sqlite')  
        df = df.loc[df['ejercicio'] == self.ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'sgf_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='sgf_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'sgf_cta_cte'], axis='columns', inplace=True)
        #Filtramos los registros duplicados en la 106
        df_106 = df.copy()
        df_106 = df_106 >> \
            dplyr.filter_(f.cta_cte == '106') >> \
            dplyr.distinct(
                f.mes, f.fecha, f.beneficiario,
                f.libramiento_sgf, 
                _keep_all=True
            )
        df = df >> \
            dplyr.filter_(f.cta_cte != '106') >> \
            dplyr.bind_rows(df_106)
        self.sgf_resumen_rend = pd.DataFrame(df)
        return self.sgf_resumen_rend

    # --------------------------------------------------
    def control_mes_cta_cte(self):
        sscc_mes_cta_cte = self.sscc_banco_invico.copy()
        sscc_mes_cta_cte = sscc_mes_cta_cte >> \
            dplyr.filter_(f.cod_imputacion != '031') >> \
            dplyr.select(f.mes, f.cta_cte, f.importe) >> \
            dplyr.group_by(f.mes, f.cta_cte) >> \
            dplyr.summarise(debitos_sscc = base.sum_(f.importe),
                            _groups = 'drop')
        sgf_mes_cta_cte = self.sgf_resumen_rend.copy()
        sgf_mes_cta_cte = sgf_mes_cta_cte >> \
            dplyr.select(
                f.mes, f.cta_cte, 
                f.importe_neto, f.retenciones
            ) >> \
            dplyr.group_by(f.mes, f.cta_cte) >> \
            dplyr.summarise(
                neto_sgf = base.sum_(f.importe_neto),
                retenciones_sgf = base.sum_(f.retenciones),
                _groups = 'drop')
        control_mes_cta_cte = sgf_mes_cta_cte >> \
            dplyr.full_join(sscc_mes_cta_cte) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            ) >> \
            dplyr.mutate(
                diferencia = f.neto_sgf + f.retenciones_sgf - f.debitos_sscc
            ) >> \
            dplyr.filter_(~dplyr.near(f.diferencia, 0))
        control_mes_cta_cte.sort_values(by=['mes', 'cta_cte'], inplace= True)
        control_mes_cta_cte = pd.DataFrame(control_mes_cta_cte)
        control_mes_cta_cte.reset_index(drop=True, inplace=True)
        return control_mes_cta_cte

    # --------------------------------------------------
    def control_completo(self):
        sscc_completo = self.sscc_banco_invico.copy()
        sscc_completo = sscc_completo >> \
            dplyr.rename(
                debitos_sscc = f.importe
            )
            # dplyr.select(
            #     f.mes, f.fecha, f.cta_cte, 
            #     f.libramiento, f.beneficiario, 
            #     debitos_sscc = f.importe
            # )
        sgf_completo = self.sgf_resumen_rend.copy()
        sgf_completo = sgf_completo >> \
            dplyr.rename(
                libramiento = f.libramiento_sgf,
                neto_sgf = f.importe_neto, 
                retenciones_sgf = f.retenciones,
            )
            # dplyr.select(
            #     f.mes, f.fecha, f.cta_cte, 
            #     f.beneficiario, 
            #     libramiento = f.libramiento_sgf,
            #     neto_sgf = f.importe_neto, 
            #     retenciones_sgf = f.retenciones,
            # )
        control_completo = sgf_completo >> \
            dplyr.full_join(sscc_completo) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            ) >> \
            dplyr.mutate(
                diferencia = f.neto_sgf + f.retenciones_sgf - f.debitos_sscc
            )
        control_completo.sort_values(
            by=['mes', 'fecha','cta_cte', 'beneficiario'], 
            inplace= True)
        control_completo = pd.DataFrame(control_completo)
        control_completo.reset_index(drop=True, inplace=True)
        return control_completo