#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
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
import numpy as np
from datar import base, dplyr, f, tidyr
from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicodb.update import update_db


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
    def import_acum_2007(self):
        file_path = os.path.join(
            self.get_update_path_input(), 
            'Reportes SIIF', 'Obras 2007 Unificado para Exportar (Depurado).xlsx'
        )
        df = pd.read_excel(file_path, dtype=str)
        df['desc_prog'] = np.where(
            df['proy'].isna(), 
            df['prog'] + " - " +  df['Descripción'],
            np.nan
        )
        df['desc_prog'] = df['desc_prog'].ffill()
        df['desc_proy'] = np.where(
            df['obra'].isna(), 
            df['proy'] + " - " +  df['Descripción'],
            np.nan
        )
        df['desc_proy'] = df['desc_proy'].ffill()
        df['desc_act'] = np.where(
            ~df['estructura'].isna(), 
            df['obra'] + " - " +  df['Descripción'],
            np.nan
        )
        df = df.dropna(subset=['estructura'])
        df['acum_2007'] = df['acum_2007'].astype(float)
        df = df.loc[:, [
            'desc_prog', 'desc_proy', 'desc_act', 
            'actividad', 'partida', 'alta', 'acum_2007'
        ]]
        return df

    # --------------------------------------------------
    def import_siif_obras_desc(self):
        df = super().import_siif_rf602(self.ejercicio)
        df = df.loc[df['partida'].isin(['421', '422'])]
        #df = df.loc[df['ordenado'] > 0]
        df = df.sort_values(by=['ejercicio', 'estructura'], ascending=[False, True])
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
    def import_siif_obras_desc_icaro(self):
        df = super().import_siif_rf602(ejercicio=self.ejercicio)
        df = df.loc[df['partida'].isin(['421', '422'])]
        df = df.loc[df['ordenado'] > 0]
        df.drop(
            labels=['org', 'pendiente', 'programa', 
            'subprograma', 'proyecto', 'actividad', 
            'grupo'], 
            axis=1, inplace=True
            )
        df['actividad'] = df['estructura'].str[0:11]
        df.sort_values(by=['ejercicio', 'estructura'], ascending=[False, True],inplace=True)
        df = df.merge(self.import_icaro_desc_pres(), how='left', on='actividad', copy=False)
        df['estructura'] = df['actividad']
        df.drop(
            labels=['ejercicio', 'actividad', 'desc_subprog'], 
            axis=1, inplace=True
            )
        df.rename(columns={
            'desc_prog':'prog_con_desc',
            'desc_proy':'proy_con_desc',
            'desc_act':'act_con_desc',
        }, inplace=True)
        df = df >>\
            dplyr.select(
                f.estructura, f.partida, f.fuente,
                f.prog_con_desc,  
                f.proy_con_desc, f.act_con_desc,
                dplyr.everything()
            )
        df = pd.DataFrame(df)
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def import_icaro_carga_desc(
        self, es_desc_siif:bool = True
    ):
        df = super().import_icaro_carga(neto_pa6=True)
        df = df.loc[df['partida'].isin(['421', '422'])]
        if isinstance(self.ejercicio, list):
            df = df.loc[df['ejercicio'].isin(self.ejercicio)]
        else:
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
        prov = super().import_icaro_proveedores()
        prov = prov.loc[:, ['cuit', 'desc_prov']]
        prov.drop_duplicates(subset=['cuit'], inplace=True)
        prov.rename(columns={'desc_prov':'proveedor'}, inplace=True)
        df = df.merge(prov, how='left', on='cuit', copy=False)
        return df

    # --------------------------------------------------
    def import_icaro_mod_basicos(
            self, es_desc_siif:bool = True, 
            neto_reg:bool = False, neto_pa6:bool = True
    ):
        df = super().import_icaro_carga(neto_reg=neto_reg, neto_pa6=neto_pa6)
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
        prov = super().import_icaro_proveedores()
        prov = prov.loc[:, ['cuit', 'desc_prov']]
        prov.drop_duplicates(subset=['cuit'], inplace=True)
        prov.rename(columns={'desc_prov':'proveedor'}, inplace=True)
        df = df.merge(prov, how='left', on='cuit', copy=False)
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
        self, full_icaro:bool = False, 
        es_desc_siif:bool = True,
        desagregar_partida:bool = True,
        desagregar_fuente:bool = True,
        ultimos_ejercicios:str = 'All'):
        df = self.import_icaro_carga_desc(es_desc_siif=es_desc_siif)
        df.sort_values(["actividad", "partida", "fuente"], inplace=True)
        group_cols = [
            "desc_prog", "desc_proy", "desc_act",
            "actividad"
        ]
        if full_icaro:
            group_cols = group_cols + ['obra']
        if desagregar_partida:
            group_cols = group_cols + ['partida']
        if desagregar_fuente:
            group_cols = group_cols + ['fuente']
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
        if ultimos_ejercicios != 'All':
            ejercicios = int(ultimos_ejercicios)
            ejercicios = df_anos.sort_values('ejercicio', ascending=False).ejercicio.unique()[0:ejercicios]
            df_anos = df_anos.loc[df_anos.ejercicio.isin(ejercicios)]
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

   # --------------------------------------------------
    def reporte_planillometro_contabilidad(
        self, es_desc_siif:bool = True,
        ultimos_ejercicios:str = 'All',
        desagregar_partida:bool = True,):
        df = self.import_icaro_carga_desc(es_desc_siif=es_desc_siif)
        df.sort_values(["actividad", "partida", "fuente"], inplace=True)
        group_cols = [
            "desc_prog", "desc_proy", "desc_act",
            "actividad"
        ]
        if desagregar_partida:
            group_cols = group_cols + ['partida']

        # Eliminamos aquellos ejercicios anteriores a 2008
        df = df.loc[df.ejercicio.astype(int) >= 2008]
        print(df.columns)
        print(df.head(1))
        # Agregamos ejecución acumulada de Patricia
        df_acum_2007 = self.import_acum_2007()
        df_acum_2007['ejercicio'] = '2007'
        df_acum_2007['avance'] = 1
        df_acum_2007['obra'] = df_acum_2007['desc_act']
        df_acum_2007 = df_acum_2007.rename(columns={'acum_2007':'importe'})
        df = pd.concat([df, df_acum_2007])

        # Ejercicio alta
        df_alta = df.groupby(group_cols).ejercicio.min().reset_index()
        df_alta.rename(columns={'ejercicio':'alta'}, inplace=True)

        df_ejercicios = df.copy()
        if ultimos_ejercicios != 'All':
            ejercicios = int(ultimos_ejercicios)
            ejercicios = df_ejercicios.sort_values('ejercicio', ascending=False).ejercicio.unique()[0:ejercicios]
            # df_anos = df_anos.loc[df_anos.ejercicio.isin(ejercicios)]
        else:
            ejercicios = df_ejercicios.sort_values('ejercicio', ascending=False).ejercicio.unique()

        # Ejercicio actual
        df_ejec_actual = df.copy()
        df_ejec_actual = df_ejec_actual.loc[df_ejec_actual.ejercicio.isin(ejercicios)]
        df_ejec_actual = df_ejec_actual.groupby(group_cols + ['ejercicio']).importe.sum().reset_index()
        df_ejec_actual.rename(columns={'importe':'ejecucion'}, inplace=True)

        # Ejecucion Acumulada
        df_acum = pd.DataFrame()
        for ejercicio in ejercicios:
            df_ejercicio = df.copy()
            df_ejercicio = df_ejercicio.loc[df_ejercicio.ejercicio.astype(int) <= int(ejercicio)]
            df_ejercicio['ejercicio'] = ejercicio
            df_ejercicio = df_ejercicio.groupby(group_cols + ['ejercicio']).importe.sum().reset_index()
            df_ejercicio.rename(columns={'importe':'acum'}, inplace=True)
            df_acum = pd.concat([df_acum, df_ejercicio])

        # Obras en curso
        df_curso = pd.DataFrame()
        for ejercicio in ejercicios:
            df_ejercicio = df.copy()
            df_ejercicio = df_ejercicio.loc[df_ejercicio.ejercicio.astype(int) <= int(ejercicio)]
            df_ejercicio['ejercicio'] = ejercicio
            obras_curso = df_ejercicio.groupby(["obra"]).avance.max().to_frame()
            obras_curso = obras_curso.loc[obras_curso.avance < 1].reset_index().obra
            df_ejercicio = df_ejercicio.loc[
                df_ejercicio.obra.isin(obras_curso)
            ].groupby(group_cols + ['ejercicio']).importe.sum().reset_index()
            df_ejercicio.rename(columns={'importe':'en_curso'}, inplace=True)
            df_curso = pd.concat([df_curso, df_ejercicio])

        # Obras terminadas anterior
        df_term_ant = pd.DataFrame()
        for ejercicio in ejercicios:
            df_ejercicio = df.copy()
            df_ejercicio = df_ejercicio.loc[df_ejercicio.ejercicio.astype(int) < int(ejercicio)]
            df_ejercicio['ejercicio'] = ejercicio
            obras_term_ant = df_ejercicio.groupby(["obra"]).avance.max().to_frame()
            obras_term_ant = obras_term_ant.loc[obras_term_ant.avance == 1].reset_index().obra
            df_ejercicio = df_ejercicio.loc[
                df_ejercicio.obra.isin(obras_term_ant)
            ].groupby(group_cols + ['ejercicio']).importe.sum().reset_index()
            df_ejercicio.rename(columns={'importe':'terminadas_ant'}, inplace=True)
            df_term_ant = pd.concat([df_term_ant, df_ejercicio])
        
        df = pd.merge(df_alta, df_acum, on=group_cols, how='left')
        df = pd.merge(df, df_ejec_actual, on= group_cols + ['ejercicio'], how='left')
        cols = df.columns.tolist()
        penultima_col = cols.pop(-2)  # Elimina la penúltima columna y la guarda
        cols.append(penultima_col)  # Agrega la penúltima columna al final
        df = df[cols]  # Reordena las columnas
        df = pd.merge(df, df_curso, on=group_cols + ['ejercicio'], how='left')
        df = pd.merge(df, df_term_ant, on=group_cols + ['ejercicio'], how='left')
        df = df.fillna(0)
        df['terminadas_actual'] = df.acum - df.en_curso - df.terminadas_ant
        df['actividad'] = df['actividad'] + '-' + df['partida']
        df = df.rename(columns={'actividad':'estructura'})
        df = df.drop(columns=['partida'])
        return df