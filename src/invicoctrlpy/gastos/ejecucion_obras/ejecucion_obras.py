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
from invicodb import update_db


@dataclass
# --------------------------------------------------
class EjecucionObras(ImportDataFrame):
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

        update_icaro = update_db.UpdateIcaro(
            os.path.dirname(os.path.dirname(self.db_path)) + 
            '/R Output/SQLite Files/ICARO.sqlite', 
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
        self.siif_desc_pres = self.import_siif_desc_pres(ejercicio_to=self.ejercicio)

    # --------------------------------------------------
    def import_siif_obras_desc(self):
        df = super().import_siif_rf602()
        df = df.loc[df['partida'].isin(['421', '422'])]
        df = df.loc[df['ordenado'] > 0]
        df.sort_values(by=['ejercicio', 'estructura'], ascending=[False, True],inplace=True)
        df = df.merge(self.siif_desc_pres, how='left', on='estructura', copy=False)
        df.drop(
            labels=['org', 'pendiente', 'programa', 
            'subprograma', 'proyecto', 'actividad', 
            'grupo'], 
            axis=1, inplace=True
            )
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
    def import_icaro_carga_desc(self, es_desc_siif:bool = True):
        df = super().import_icaro_carga(neto_pa6=True)
        df = df.loc[df['partida'].isin(['421', '422'])]
        df = df.loc[df.ejercicio.astype(int) <= int(self.ejercicio)]
        if es_desc_siif:
            df['estructura'] = df['actividad'] + '-' + df['partida']
            df = df.merge(self.siif_desc_pres, how='left', on='estructura', copy=False)
            df.drop(labels=['estructura'], axis='columns', inplace=True)
        else:
            df = df.merge(
                self.import_icaro_desc_pres(), how='left', 
                on='actividad', copy=False
            )
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def import_icaro_mod_basicos(self, es_desc_siif:bool = True):
        df = super().import_icaro_carga(neto_reg=False, neto_pa6=True)
        df = df.loc[df['actividad'].str.startswith('29')]
        df = df.loc[df.ejercicio.astype(int) <= int(self.ejercicio)]
        df_obras = super().import_icaro_obras()
        df_obras = df_obras.loc[:, ['obra', 'localidad', 'norma_legal', 'info_adicional']]
        df = df.merge(df_obras, how='left', on='obra', copy=False)
        if es_desc_siif:
            df['estructura'] = df['actividad'] + '-' + df['partida']
            df = df.merge(self.siif_desc_pres, how='left', on='estructura', copy=False)
            df.drop(labels=['estructura'], axis='columns', inplace=True)
        else:
            df = df.merge(
                self.import_icaro_desc_pres(), how='left', 
                on='actividad', copy=False
            )
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def reporte_icaro_mod_basicos(
        self, por_convenio:bool = True, es_desc_siif:bool = True):
        df = self.import_icaro_mod_basicos(es_desc_siif=es_desc_siif)
        if por_convenio:
            df = df.loc[df.fuente == '11']
        df.sort_values(["actividad", "partida", "obra", "fuente"], inplace=True)
        group_cols = [
            "desc_prog", "desc_proy", "desc_act",
            "actividad", "partida", "fuente",
            "localidad", "norma_legal", "obra"
        ]
        # Ejercicio alta
        df_alta = df.groupby(group_cols).ejercicio.min().reset_index()
        df_alta.rename(columns={'ejercicio':'alta'}, inplace=True)
        # Ejecucion Total
        df_total = df.groupby(group_cols).importe.sum().reset_index()
        df_total.rename(columns={'importe':'ejecucion_total'}, inplace=True)
        # Obras en curso
        obras_curso = df.groupby(["obra"]).avance.max().to_frame()
        obras_curso = obras_curso.loc[obras_curso.avance < 1].reset_index().obra
        df_curso = df.loc[df.obra.isin(obras_curso)].groupby(group_cols).importe.sum().reset_index()
        df_curso.rename(columns={'importe':'en_curso'}, inplace=True)
        # Obras terminadas anterior
        df_prev = df.loc[df.ejercicio.astype(int) < int(self.ejercicio)]
        obras_term_ant = df_prev.loc[df_prev.avance == 1].obra
        df_term_ant = df_prev.loc[df_prev.obra.isin(obras_term_ant)].groupby(group_cols).importe.sum().reset_index()
        df_term_ant.rename(columns={'importe':'terminadas_ant'}, inplace=True)
        # Pivoteamos en funcion de...
        if por_convenio:
            df_pivot = df.loc[:,
                group_cols +
                ["info_adicional", "importe"]]
            df_pivot = df_pivot >>\
                tidyr.pivot_wider(
                    names_from= f.info_adicional,
                    values_from = f.importe,
                    values_fn = base.sum,
                    values_fill = 0
                )
        else:
            df_pivot = df.loc[:,
                group_cols +
                ["ejercicio", "importe"]]
            df_pivot = df_pivot >>\
                tidyr.pivot_wider(
                    names_from= f.ejercicio,
                    values_from = f.importe,
                    values_fn = base.sum,
                    values_fill = 0
                )          

        # Agrupamos todo
        df = df_alta >>\
            dplyr.left_join(df_pivot) >>\
            dplyr.left_join(df_total) >>\
            dplyr.left_join(df_curso) >>\
            dplyr.left_join(df_term_ant) >>\
            tidyr.replace_na(0) >>\
            dplyr.mutate(
                terminadas_actual = f.ejecucion_total - f.en_curso - f.terminadas_ant)
        df = pd.DataFrame(df)
        df.reset_index(drop=True, inplace=True)
        df.rename(columns = {'': 'Sin Convenio'}, inplace = True)
        return df

    # --------------------------------------------------
    def reporte_siif_ejec_obras_actual(self):
        df = self.import_siif_obras_desc()
        df = df.loc[df.ejercicio == self.ejercicio]
        df.drop(labels=['ejercicio'], axis=1, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def reporte_planillometro(
        self, full_icaro:bool = False, es_desc_siif:bool = True):
        df = self.import_icaro_carga_desc(es_desc_siif=es_desc_siif)
        df.sort_values(["actividad", "partida", "fuente"], inplace=True)
        group_cols = [
            "desc_prog", "desc_proy", "desc_act",
            "actividad", "partida", "fuente",
        ]
        if full_icaro:
            group_cols = group_cols + ['obra']
        # Ejercicio alta
        df_alta = df.groupby(group_cols).ejercicio.min().reset_index()
        df_alta.rename(columns={'ejercicio':'alta'}, inplace=True)
        # Ejecucion Acumulada
        df_acum = df.groupby(group_cols).importe.sum().reset_index()
        df_acum.rename(columns={'importe':'acum'}, inplace=True)
        # Obras en curso
        obras_curso = df.groupby(["obra"]).avance.max().to_frame()
        obras_curso = obras_curso.loc[obras_curso.avance < 1].reset_index().obra
        df_curso = df.loc[df.obra.isin(obras_curso)].groupby(group_cols).importe.sum().reset_index()
        df_curso.rename(columns={'importe':'en_curso'}, inplace=True)
        # Obras terminadas anterior
        df_prev = df.loc[df.ejercicio.astype(int) < int(self.ejercicio)]
        obras_term_ant = df_prev.loc[df_prev.avance == 1].obra
        df_term_ant = df_prev.loc[df_prev.obra.isin(obras_term_ant)].groupby(group_cols).importe.sum().reset_index()
        df_term_ant.rename(columns={'importe':'terminadas_ant'}, inplace=True)
        # Pivoteamos en funcion del ejercicio
        df_anos = df.loc[:,
            group_cols +
            ["ejercicio", "importe"]]
        df_anos = df_anos >>\
            tidyr.pivot_wider(
                names_from= f.ejercicio,
                values_from = f.importe,
                values_fn = base.sum,
                values_fill = 0
            )

        # Agrupamos todo
        df = df_alta >>\
            dplyr.left_join(df_anos) >>\
            dplyr.left_join(df_acum) >>\
            dplyr.left_join(df_curso) >>\
            dplyr.left_join(df_term_ant) >>\
            tidyr.replace_na(0) >>\
            dplyr.mutate(
                terminadas_actual = f.acum - f.en_curso - f.terminadas_ant)
        df = pd.DataFrame(df)
        df.reset_index(drop=True, inplace=True)
        return df
