#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: SGF vs SSCC
Data required:
    - Icaro
    - SIIF rdeu012
    - SGF Resumen de Rendiciones por Proveedor
    - SGF Listado Proveedores
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
from dataclasses import dataclass

import pandas as pd
import update_db
from datar import base, dplyr, f, tidyr

from invicoctrlpy.utils.import_dataframe import ImportDataFrame


@dataclass
# --------------------------------------------------
class ControlObras(ImportDataFrame):
    ejercicio:str = str(dt.datetime.now().year)
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
        update_path_input = self.get_update_path_input()

        update_icaro = update_db.UpdateIcaro(
            self.get_outside_path() + '/R Output/SQLite Files/ICARO.sqlite', 
            self.db_path + '/icaro.sqlite')
        update_icaro.migrate_icaro()

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_deuda_flotante_rdeu012()

        update_sgf = update_db.UpdateSGF(
            update_path_input + '/Sistema Gestion Financiera', 
            self.db_path + '/sgf.sqlite')
        update_sgf.update_resumen_rend_prov()
        update_sgf.update_listado_prov()
        
        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.import_icaro_neto_rdeu(self.ejercicio)
        self.import_siif_rdeu012()
        self.import_icaro(self.ejercicio)
        self.import_resumen_rend_cuit()

    # --------------------------------------------------
    def import_resumen_rend_cuit(self):
        df = super().import_resumen_rend_cuit(self.ejercicio)
        df = df.loc[df['origen'] != 'FUNCIONAMIENTO']
        #Filtramos los registros de honorarios en EPAM
        df_epam = df.copy()
        keep = ['HONORARIOS']
        df_epam = df_epam.loc[df_epam['origen'] == 'EPAM']
        df_epam = df_epam.loc[~df_epam.destino.str.contains('|'.join(keep))]
        df = df.loc[df['origen'] != 'EPAM']
        df = pd.concat([df, df_epam], ignore_index=True)
        self.sgf_resumen_rend_cuit = pd.DataFrame(df)
        return self.sgf_resumen_rend_cuit

    # --------------------------------------------------
    def control_mes_cta_cte(self):
        icaro_mes_cta_cte = self.icaro_neto_rdeu.copy()
        icaro_mes_cta_cte = icaro_mes_cta_cte >> \
            dplyr.select(f.mes, f.cta_cte, f.importe) >> \
            dplyr.group_by(f.mes, f.cta_cte) >> \
            dplyr.summarise(ejecutado_icaro = base.sum_(f.importe),
                            _groups = 'drop')
        sgf_mes_cta_cte = self.sgf_resumen_rend_cuit.copy()
        sgf_mes_cta_cte = sgf_mes_cta_cte >> \
            dplyr.select(
                f.mes, f.cta_cte, 
                f.importe_bruto
            ) >> \
            dplyr.group_by(f.mes, f.cta_cte) >> \
            dplyr.summarise(
                bruto_sgf = base.sum_(f.importe_bruto),
                _groups = 'drop')
        control_mes_cta_cte = icaro_mes_cta_cte >> \
            dplyr.full_join(sgf_mes_cta_cte) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            ) >> \
            dplyr.mutate(
                diferencia = f.ejecutado_icaro - f.bruto_sgf
            )
        #     dplyr.filter_(~dplyr.near(f.diferencia, 0))
        control_mes_cta_cte.sort_values(by=['mes', 'cta_cte'], inplace= True)
        control_mes_cta_cte = pd.DataFrame(control_mes_cta_cte)
        control_mes_cta_cte.reset_index(drop=True, inplace=True)
        return control_mes_cta_cte

    # --------------------------------------------------
    def control_completo(self):
        icaro_completo = self.icaro_neto_rdeu.copy()
        icaro_completo = icaro_completo >> \
            dplyr.rename_with(lambda x: 'icaro_' + x) >> \
            dplyr.rename(
                ejercicio = f.icaro_ejercicio,
                mes = f.icaro_mes,
                fecha = f.icaro_fecha,
                cta_cte = f.icaro_cta_cte,
                cuit = f.icaro_cuit,
            )
        sgf_completo = self.sgf_resumen_rend_cuit.copy()
        sgf_completo = sgf_completo >> \
            dplyr.rename_with(lambda x: 'sgf_' + x) >> \
            dplyr.rename(
                ejercicio = f.sgf_ejercicio,
                mes = f.sgf_mes,
                fecha = f.sgf_fecha,
                cta_cte = f.sgf_cta_cte,
                cuit = f.sgf_cuit,
            )
            # dplyr.select(
            #     f.mes, f.fecha, f.cta_cte, 
            #     f.beneficiario, 
            #     libramiento = f.libramiento_sgf,
            #     neto_sgf = f.importe_neto, 
            #     retenciones_sgf = f.retenciones,
            # )
        control_completo = sgf_completo >> \
            dplyr.full_join(icaro_completo) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            ) >> \
            dplyr.mutate(
                diferencia = f.icaro_importe - f.sgf_importe_bruto
            )
        control_completo.sort_values(
            by=['mes', 'fecha','cta_cte', 'cuit'], 
            inplace= True)
        control_completo = pd.DataFrame(control_completo)
        control_completo.reset_index(drop=True, inplace=True)
        return control_completo

# Ajuste Reemplazo de Chq y Cert Neg???