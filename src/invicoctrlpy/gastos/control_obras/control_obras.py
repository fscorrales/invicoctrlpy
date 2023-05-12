#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: SGF vs SSCC
Data required:
    - Icaro
    - SIIF rdeu012
    - SGF Resumen de Rendiciones por Proveedor
    - SGF Listado Proveedores
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
import os
from dataclasses import dataclass

import pandas as pd
from datar import base, dplyr, f, tidyr
from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicodb.update import update_db


@dataclass
# --------------------------------------------------
class ControlObras(ImportDataFrame):
    ejercicio:str = str(dt.datetime.now().year)
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
        update_sscc.update_banco_invico()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        # self.import_icaro_carga_neto_rdeu(self.ejercicio)
        # self.import_siif_rdeu012()
        self.import_icaro_carga(self.ejercicio)
        self.import_resumen_rend_cuit()

    # --------------------------------------------------
    def import_resumen_rend_cuit(self):
        df = super().import_resumen_rend_cuit(
            self.ejercicio, neto_cert_neg=True)
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
    def control_cruzado(
        self, groupby_cols:list = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:
        icaro = self.import_icaro_carga_neto_rdeu(self.ejercicio).copy()
        icaro = icaro.loc[:, groupby_cols + ['importe']]
        icaro = icaro.groupby(groupby_cols)['importe'].sum()
        icaro = icaro.reset_index()
        icaro = icaro.rename(columns={'importe':'ejecutado_icaro'})
        # icaro = icaro >> \
        #     dplyr.select(f.mes, f.cta_cte, f.importe) >> \
        #     dplyr.group_by(f.mes, f.cta_cte) >> \
        #     dplyr.summarise(ejecutado_icaro = base.sum_(f.importe),
        #                     _groups = 'drop')
        sgf = self.sgf_resumen_rend_cuit.copy()
        sgf = sgf.loc[:, groupby_cols + ['importe_bruto']]
        sgf = sgf.groupby(groupby_cols)['importe_bruto'].sum()
        sgf = sgf.reset_index()
        sgf = sgf.rename(columns={'importe_bruto':'bruto_sgf'})
        # sgf = sgf >> \
        #     dplyr.select(
        #         f.mes, f.cta_cte, 
        #         f.importe_bruto
        #     ) >> \
        #     dplyr.group_by(f.mes, f.cta_cte) >> \
        #     dplyr.summarise(
        #         bruto_sgf = base.sum_(f.importe_bruto),
        #         _groups = 'drop')
        df = pd.merge(icaro, sgf, how='outer', on=groupby_cols, copy=False)
        df[['ejecutado_icaro', 'bruto_sgf']] = df[['ejecutado_icaro', 'bruto_sgf']].fillna(0)
        df['diferencia'] = df.ejecutado_icaro - df.bruto_sgf
        # df = icaro >> \
        #     dplyr.full_join(sgf) >> \
        #     dplyr.mutate(
        #         dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
        #     ) >> \
        #     dplyr.mutate(
        #         diferencia = f.ejecutado_icaro - f.bruto_sgf
        #     )
        #     dplyr.filter_(~dplyr.near(f.diferencia, 0))
        # df.sort_values(by=['mes', 'cta_cte'], inplace= True)
        # df['dif_acum'] = df['diferencia'].cumsum()
        df = pd.DataFrame(df)
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def control_completo(self):
        icaro = self.import_icaro_carga_neto_rdeu(self.ejercicio).copy()
        icaro = icaro.add_prefix('icaro_')
        icaro = icaro.rename(columns={
            'icaro_ejercicio':'ejercicio',
            'icaro_mes':'mes',
            'icaro_cta_cte':'cta_cte',
        })
        # icaro = icaro >> \
        #     dplyr.rename_with(lambda x: 'icaro_' + x) >> \
        #     dplyr.rename(
        #         ejercicio = f.icaro_ejercicio,
        #         mes = f.icaro_mes,
        #         fecha = f.icaro_fecha,
        #         cta_cte = f.icaro_cta_cte,
        #         cuit = f.icaro_cuit,
        #     )
        sgf = self.sgf_resumen_rend_cuit.copy()
        sgf = sgf.add_prefix('sgf_')
        sgf = sgf.rename(columns={
            'sgf_ejercicio':'ejercicio',
            'sgf_mes':'mes',
            'sgf_cta_cte':'cta_cte',
        })
        # sgf = sgf >> \
        #     dplyr.rename_with(lambda x: 'sgf_' + x) >> \
        #     dplyr.rename(
        #         ejercicio = f.sgf_ejercicio,
        #         mes = f.sgf_mes,
        #         fecha = f.sgf_fecha,
        #         cta_cte = f.sgf_cta_cte,
        #         cuit = f.sgf_cuit,
        #     )
        groupby = ['ejercicio', 'mes', 'cta_cte']
        df = pd.merge(icaro, sgf, how='outer', on=groupby, copy=False)
        df[['icaro_importe', 'sgf_importe_bruto']] = df[['icaro_importe', 'sgf_importe_bruto']].fillna(0)
        df['diferencia'] = df.icaro_importe - df.sgf_importe_bruto
        # df = sgf >> \
        #     dplyr.full_join(icaro) >> \
        #     dplyr.mutate(
        #         dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
        #     ) >> \
        #     dplyr.mutate(
        #         diferencia = f.icaro_importe - f.sgf_importe_bruto
        #     )
        df = df.sort_values(by=groupby)
        df = df.reset_index(drop=True)
        return df

# Ajuste Reemplazo de Chq y Cert Neg???
