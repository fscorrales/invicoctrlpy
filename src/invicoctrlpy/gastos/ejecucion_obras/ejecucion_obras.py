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
from dataclasses import dataclass, field

import pandas as pd
import update_db
from datar import base, dplyr, f, tidyr

from invicoctrlpy.utils.import_dataframe import ImportDataFrame


@dataclass
# --------------------------------------------------
class EjecucionObras(ImportDataFrame):
    ejercicio:str = str(dt.datetime.now().year)
    db_path:str = None
    update_db:bool = False
    siif_desc_pres:pd.DataFrame = field(init=False, repr=False)
    siif_ejec_obras:pd.DataFrame = field(init=False, repr=False)
    icaro_carga_desc:pd.DataFrame = field(init=False, repr=False)
    icaro_mod_basicos:pd.DataFrame = field(init=False, repr=False)

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
        update_siif.update_ppto_gtos_fte_rf602()
        update_siif.update_ppto_gtos_desc_rf610()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.siif_desc_pres = self.import_siif_desc_pres()
        self.import_siif_obras()
        self.import_icaro_carga_desc()
        self.import_icaro_mod_basicos()

    # --------------------------------------------------
    def import_siif_obras(self):
        df = super().import_siif_rf602()
        df = df.loc[df['partida'].isin(['421', '422'])]
        df = df.loc[df['ordenado'] > 0]
        df = df.merge(self.siif_desc_pres, how='left', on='estructura', copy=False)
        self.siif_ejec_obras = df

    # --------------------------------------------------
    def import_icaro_carga_desc(self):
        df = super().import_icaro_carga(neto_pa6=True)
        df = df.loc[df.ejercicio.astype(int) <= int(self.ejercicio)]
        df['estructura'] = df['actividad'] + '-' + df['partida']
        df = df.merge(self.siif_desc_pres, how='left', on='estructura', copy=False)
        df.drop(labels=['estructura'], axis='columns', inplace=True)
        self.icaro_carga_desc = df

    # --------------------------------------------------
    def import_icaro_mod_basicos(self):
        df = super().import_icaro_carga(self.ejercicio, neto_pa6=False, neto_reg=True)
        df = df.loc[df.ejercicio.astype(int) <= int(self.ejercicio)]
        df = df.loc[df['actividad'].str.startswith('29')]
        df_obras = super().import_icaro_obras()
        df_obras = df_obras.loc[:, ['obra', 'localidad', 'norma_legal', 'info_adicional']]
        df = df.merge(df_obras, how='left', on='obra', copy=False)
        df['estructura'] = df['actividad'] + '-' + df['partida']
        df = df.merge(self.siif_desc_pres, how='left', on='estructura', copy=False)
        df.drop(labels=['estructura'], axis='columns', inplace=True)
        self.icaro_mod_basicos = df

    # --------------------------------------------------
    def reporte_siif_ejec_obras(self):
        df = self.siif_ejec_obras.copy()
        df.drop(labels=['ejercicio'], axis=1, inplace=True)
        return df

#     # --------------------------------------------------
#     def control_mes_cta_cte(self):
#         icaro_mes_cta_cte = self.icaro_carga_neto_rdeu.copy()
#         icaro_mes_cta_cte = icaro_mes_cta_cte >> \
#             dplyr.select(f.mes, f.cta_cte, f.importe) >> \
#             dplyr.group_by(f.mes, f.cta_cte) >> \
#             dplyr.summarise(ejecutado_icaro = base.sum_(f.importe),
#                             _groups = 'drop')
#         sgf_mes_cta_cte = self.sgf_resumen_rend_cuit.copy()
#         sgf_mes_cta_cte = sgf_mes_cta_cte >> \
#             dplyr.select(
#                 f.mes, f.cta_cte, 
#                 f.importe_bruto
#             ) >> \
#             dplyr.group_by(f.mes, f.cta_cte) >> \
#             dplyr.summarise(
#                 bruto_sgf = base.sum_(f.importe_bruto),
#                 _groups = 'drop')
#         control_mes_cta_cte = icaro_mes_cta_cte >> \
#             dplyr.full_join(sgf_mes_cta_cte) >> \
#             dplyr.mutate(
#                 dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
#             ) >> \
#             dplyr.mutate(
#                 diferencia = f.ejecutado_icaro - f.bruto_sgf
#             )
#         #     dplyr.filter_(~dplyr.near(f.diferencia, 0))
#         control_mes_cta_cte.sort_values(by=['mes', 'cta_cte'], inplace= True)
#         control_mes_cta_cte['dif_acum'] = control_mes_cta_cte['diferencia'].cumsum()
#         control_mes_cta_cte = pd.DataFrame(control_mes_cta_cte)
#         control_mes_cta_cte.reset_index(drop=True, inplace=True)
#         return control_mes_cta_cte

#     # --------------------------------------------------
#     def control_completo(self):
#         icaro_completo = self.icaro_carga_neto_rdeu.copy()
#         icaro_completo = icaro_completo >> \
#             dplyr.rename_with(lambda x: 'icaro_' + x) >> \
#             dplyr.rename(
#                 ejercicio = f.icaro_ejercicio,
#                 mes = f.icaro_mes,
#                 fecha = f.icaro_fecha,
#                 cta_cte = f.icaro_cta_cte,
#                 cuit = f.icaro_cuit,
#             )
#         sgf_completo = self.sgf_resumen_rend_cuit.copy()
#         sgf_completo = sgf_completo >> \
#             dplyr.rename_with(lambda x: 'sgf_' + x) >> \
#             dplyr.rename(
#                 ejercicio = f.sgf_ejercicio,
#                 mes = f.sgf_mes,
#                 fecha = f.sgf_fecha,
#                 cta_cte = f.sgf_cta_cte,
#                 cuit = f.sgf_cuit,
#             )
#             # dplyr.select(
#             #     f.mes, f.fecha, f.cta_cte, 
#             #     f.beneficiario, 
#             #     libramiento = f.libramiento_sgf,
#             #     neto_sgf = f.importe_neto, 
#             #     retenciones_sgf = f.retenciones,
#             # )
#         control_completo = sgf_completo >> \
#             dplyr.full_join(icaro_completo) >> \
#             dplyr.mutate(
#                 dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
#             ) >> \
#             dplyr.mutate(
#                 diferencia = f.icaro_importe - f.sgf_importe_bruto
#             )
#         control_completo.sort_values(
#             by=['mes', 'fecha','cta_cte', 'cuit'], 
#             inplace= True)
#         control_completo = pd.DataFrame(control_completo)
#         control_completo.reset_index(drop=True, inplace=True)
#         return control_completo

# # Ajuste Reemplazo de Chq y Cert Neg???