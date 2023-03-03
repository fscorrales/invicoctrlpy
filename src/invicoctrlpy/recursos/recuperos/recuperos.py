#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: Informe y control del Sistema de Recuperos (Gestión Vivienda)
Data required:
    - Informe Saldos Por Barrio (
        https://gv.invico.gov.ar/App/Recupero/Informes/InformeSaldosPorBarrio.aspx
    - Informe Evolucion De Saldos Por Barrio (
        https://gv.invico.gov.ar/App/Recupero/Informes/InformeEvolucionDeSaldosPorBarrio.aspx)
    - Informe Variacion Saldos Recuperos a Cobrar(
        https://gv.invico.gov.ar/App/Recupero/Informes/InformeVariacionSaldosRecuperosACobrar.aspx)
    - Informe de Evolución Saldos Por Motivos(
        https://gv.invico.gov.ar/App/Recupero/Informes/InformeEvolucionDeSaldosPorMotivos.aspx)
    - Informe Barrios Nuevos (
        https://gv.invico.gov.ar/App/Recupero/Informes/InformeBarriosNuevosIncorporados.aspx)
    - Resumen Facturado (
        https://gv.invico.gov.ar/App/Recupero/Informes/ResumenFacturado.aspx)
    - Resumen Recaudado (
        https://gv.invico.gov.ar/App/Recupero/Informes/ResumenRecaudado.aspx)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
from dataclasses import dataclass

import numpy as np
import pandas as pd
from invicodb import update_db
from datar import base, dplyr, f, tidyr

from invicoctrlpy.utils.import_dataframe import ImportDataFrame
import plotly.express as px
import seaborn as sns
sns.set_theme(style='ticks')


@dataclass
# --------------------------------------------------
class Recuperos(ImportDataFrame):
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

    # --------------------------------------------------
    def update_sql_db(self):
        if self.input_path == None:
            update_path_input = self.get_update_path_input()
        else:
            update_path_input = self.input_path

        update_recuperos = update_db.UpdateSGV(
            update_path_input + '/Gestión Vivienda GV/Sistema Recuperos GV', 
            self.db_path + '/sgv.sqlite')
        update_recuperos.update_saldo_barrio()
        update_recuperos.update_saldo_barrio_variacion()
        update_recuperos.update_saldo_recuperos_cobrar_variacion()
        update_recuperos.update_saldo_motivo()
        update_recuperos.update_saldo_motivo_entrega_viviendas()
        update_recuperos.update_barrios_nuevos()
        update_recuperos.update_resumen_facturado()
        update_recuperos.update_resumen_recaudado()

    def control_saldos_recuperos_cobrar_variacion(self):
        df = self.import_saldo_recuperos_cobrar_variacion(self.ejercicio)
        df = df.loc[df['concepto'].isin(['SALDO AL INICIO:', 'SALDO AL FINAL:'])]
        return df

    def graficar_saldos_barrio(self):
        df = self.import_saldo_barrio(self.ejercicio)
        df = df.groupby(['ejercicio']).saldo_actual.sum().to_frame()
        df.reset_index(drop=False, inplace=True) 
        df['saldo_actual'] = df['saldo_actual'] / 1000000000
        # sns.barplot(data=df, x='ejercicio', y='saldo_actual')
        fig = px.bar(
            x=df['ejercicio'], y=df['saldo_actual'],
            title = 'Gráfico 1: Recuperos a Cobrar',
            labels={'x':'Ejercicio','y':'miles de millones de $'}
        )
        fig.update_layout(showlegend = False)
        fig.show()
        # fig = px.area(
        #     y=df["saldo_actual"],x=df['ejercicio'],
        #     title = 'Recuperos',
        #     labels={'x':'Ejercicio','y':'$'}
        # )
        # fig.update_layout(showlegend = False)
        # fig.show()

    def control_suma_saldo_barrio_variacion(self):
        df = self.import_saldo_barrio_variacion(self.ejercicio)
        df = df.groupby(["ejercicio"])[["saldo_inicial", "amortizacion", "cambios", "saldo_final"]].sum()
        df["suma_algebraica"] = df.saldo_inicial + df.amortizacion + df.cambios
        df["dif_saldo_final"] = df.saldo_final - df.suma_algebraica
        return df

    def graficar_dif_saldo_final_evolucion_de_saldos(self):
        df = self.control_suma_saldo_barrio_variacion()
        df = df.groupby(['ejercicio']).dif_saldo_final.sum().to_frame()
        df.reset_index(drop=False, inplace=True) 
        df['dif_saldo_final'] = df['dif_saldo_final'] / 1000
        fig = px.area(
            x=df['ejercicio'], y=df['dif_saldo_final'],
            title = '',
            labels={'x':'Ejercicio','y':'miles de $'}
        )
        fig.update_layout(showlegend = False)
        fig.show()

    def control_saldo_final_distintos_reportes(self):
        df_var = self.import_saldo_recuperos_cobrar_variacion(self.ejercicio)
        df_var = df_var.loc[df_var['concepto'] == 'SALDO AL FINAL:', ['ejercicio', 'importe']]
        df_saldo = self.import_saldo_barrio_variacion(self.ejercicio)
        df_saldo = df_saldo.groupby(['ejercicio']).saldo_final.sum().to_frame()
        df_saldo.reset_index(drop=False, inplace=True) 
        df = pd.merge(left=df_var, right=df_saldo, how='left', on='ejercicio', copy=False)
        df['dif_saldo_final'] = df.importe - df.saldo_final
        df_saldo.reset_index(drop=True, inplace=True) 
        return df

    def graficar_dif_saldo_final_reportes(self):
        df = self.control_saldo_final_distintos_reportes()
        # df['dif_saldo_final'] = df['dif_saldo_final'] / 1000
        fig = px.bar(
            x=df['ejercicio'], y=df['dif_saldo_final'],
            title = 'Gráfico 2: Diferencia entre saldos finales',
            labels={'x':'Ejercicio','y':'$'}
        )
        fig.update_layout(showlegend = False)
        fig.show()