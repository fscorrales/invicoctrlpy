#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: Control de Retenciones Banco INVICO vs SIIF o Icaro
Data required:
    - Icaro
    - SIIF rdeu012 (para netear Icaro)
    - SGF Resumen Rend por Proveedor
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
    - SIIF rcocc31 (
        2111-1-2 Contratistas
        2122-1-2 Retenciones
        1112-2-6 Banco)
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
class ControlRetenciones(ImportDataFrame):
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
        update_siif.update_mayor_contable_rcocc31()
        update_siif.update_deuda_flotante_rdeu012()

        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_banco_invico()

        update_sgf = update_db.UpdateSGF(
            update_path_input + '/Sistema Gestion Financiera', 
            self.db_path + '/sgf.sqlite')
        update_sgf.update_resumen_rend_prov()
        update_sgf.update_listado_prov()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        # self.siif_desc_pres = self.import_siif_desc_pres(ejercicio_to=self.ejercicio)
        # self.icaro_desc_pres = self.import_icaro_desc_pres()

    # --------------------------------------------------
    def import_icaro_carga_neto_rdeu(self):
        icaro_carga = super().import_icaro_carga_neto_rdeu(ejercicio=self.ejercicio)
        return icaro_carga

    # --------------------------------------------------
    def import_icaro_retenciones(self):
        icaro_carga = super().import_icaro_carga_neto_rdeu(ejercicio=self.ejercicio)
        icaro_retenciones = super().import_icaro_retenciones()
        icaro_retenciones = icaro_retenciones.loc[
            icaro_retenciones['id_carga'].isin(icaro_carga['id'].values.tolist())]
        icaro_retenciones.reset_index(drop=True, inplace=True)
        df_pivot = icaro_retenciones >>\
            dplyr.select(~f.id) >>\
            tidyr.pivot_wider(
                names_from= f.codigo,
                values_from = f.importe,
                values_fn = base.sum,
                values_fill = 0
            )
        df_pivot = pd.DataFrame(df_pivot)
        df_pivot['retenciones'] = df_pivot.sum(axis=1, numeric_only= True)
        return pd.DataFrame(df_pivot)

    # --------------------------------------------------
    def import_icaro_carga_con_retenciones(self):
        icaro_carga = self.import_icaro_carga_neto_rdeu()
        icaro_retenciones = self.import_icaro_retenciones()
        icaro_completo = icaro_carga.merge(
            icaro_retenciones, how='left', left_on='id', 
            right_on='id_carga', copy=False)
        icaro_completo = icaro_completo >>\
            tidyr.replace_na(0) >>\
            dplyr.mutate(
                importe_bruto = f.importe,
                retenciones = dplyr.if_else(f.importe_bruto < 0, 
                    f.retenciones * (-1), f.retenciones),
                importe_neto = f.importe_bruto - f.retenciones
            ) >>\
            dplyr.select(
                ~f.importe, ~f.id_carga, ~f.nro_comprobante) >>\
            dplyr.select(
                f.ejercicio, f.mes, f.fecha, f.id, f.fuente,
                f.cta_cte, f.cuit, f.tipo, 
                f.importe_bruto, f.retenciones, f.importe_neto,
                dplyr.everything()
            )
        return icaro_completo

    # --------------------------------------------------
    def import_resumen_rend_cuit(self) -> pd.DataFrame:
        return super().import_resumen_rend_cuit(
            ejercicio = self.ejercicio, neto_cert_neg=False)

    # --------------------------------------------------
    def import_siif_pagos_contratistas(self):
        siif_banco = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '1112-2-6'
        )
        siif_banco = siif_banco >>\
            dplyr.transmute(
                nro_entrada = f.nro_entrada,
                cta_cte = f.auxiliar_1
            )
        siif_contratistas = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '2111-1-2'
        )
        siif_contratistas = siif_contratistas.loc[
            siif_contratistas['tipo_comprobante'].isin(['CAP', 'ANP', 'CAD'])]
        df = siif_contratistas >>\
            dplyr.transmute(
                ejercicio = f.ejercicio,
                mes = f.mes,
                nro_entrada = f.nro_entrada,
                tipo = f.tipo_comprobante,
                cuit = f.auxiliar_1,
                importe = f.debitos
            ) >>\
            dplyr.left_join(siif_banco, by='nro_entrada')
        return pd.DataFrame(df)

    # --------------------------------------------------
    def import_siif_pagos_retenciones(self):
        siif_banco = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '1112-2-6'
        )
        siif_banco = siif_banco >>\
            dplyr.transmute(
                nro_entrada = f.nro_entrada,
                cta_cte = f.auxiliar_1
            )
        siif_retenciones = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '2122-1-2'
        )
        siif_retenciones = siif_retenciones.loc[
            siif_retenciones['tipo_comprobante'].isin(['CAP', 'ANP', 'CAD'])]
        df = siif_retenciones >>\
            dplyr.transmute(
                ejercicio = f.ejercicio,
                mes = f.mes,
                nro_entrada = f.nro_entrada,
                tipo = f.tipo_comprobante,
                cod_ret = f.auxiliar_1,
                importe = f.debitos
            ) >>\
            dplyr.left_join(siif_banco, by='nro_entrada') 
        return pd.DataFrame(df)

    # --------------------------------------------------
    def import_banco_invico(self) -> pd.DataFrame:
        return super().import_banco_invico(ejercicio = self.ejercicio)

    # --------------------------------------------------
    def control_retenciones_mensual(self):
        icaro_carga = self.import_icaro_carga_neto_rdeu()
        icaro_retenciones = self.import_icaro_retenciones()
        df = icaro_carga >>\
            dplyr.select(
                f.mes, f.id, importe_bruto = f.importe
            ) >>\
            dplyr.left_join(
                icaro_retenciones,
                by={'id':'id_carga'},
                copy=False
            ) >>\
            dplyr.select(~f.id) >>\
            dplyr.mutate(
                retenciones = dplyr.if_else(f.importe_bruto < 0, 
                    f.retenciones * (-1), f.retenciones)
                )
        df = pd.DataFrame(df)
        df = df.groupby(['mes']).sum()
        df.reset_index(inplace=True)
        #df['retenciones'] = df.sum(axis=1, numeric_only= True) - df.importe_bruto
        df['importe_neto'] = df.importe_bruto - df.retenciones
        return df

    # --------------------------------------------------
    def control_retenciones_mensual_cta_cte(self):
        icaro_carga = self.import_icaro_carga_neto_rdeu()
        icaro_retenciones = self.import_icaro_retenciones()
        df = icaro_carga >>\
            dplyr.select(
                f.mes, f.cta_cte, f.id, importe_bruto = f.importe
            ) >>\
            dplyr.left_join(
                icaro_retenciones,
                by={'id':'id_carga'},
                copy=False
            ) >>\
            dplyr.select(~f.id) >>\
            dplyr.mutate(
                retenciones = dplyr.if_else(f.importe_bruto < 0, 
                    f.retenciones * (-1), f.retenciones)
                )
        df = pd.DataFrame(df)
        df = df.groupby(['mes', 'cta_cte']).sum()
        df.reset_index(inplace=True)
        #df['retenciones'] = df.sum(axis=1, numeric_only= True) - df.importe_bruto
        df['importe_neto'] = df.importe_bruto - df.retenciones
        return df
