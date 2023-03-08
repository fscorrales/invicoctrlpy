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

    # --------------------------------------------------
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
            title = 'Recuperos a Cobrar',
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

    # --------------------------------------------------
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

    # --------------------------------------------------
    def saldo_motivo_mas_amort(self) -> pd.DataFrame:
        df = super().import_saldo_motivo(self.ejercicio)
        # amort = self.control_suma_saldo_barrio_variacion()
        # amort.reset_index(drop=False, inplace=True)
        # amort = amort.loc[:, ['ejercicio', 'amortizacion']]
        # amort['cod_motivo'] = '-'
        # amort['motivo'] = 'AMORTIZACION'
        # amort.rename(columns={'amortizacion':'importe'}, inplace=True, copy=False)
        amort = super().import_resumen_recaudado(self.ejercicio)
        amort = amort.groupby(["ejercicio"])[["amortizacion"]].sum()
        amort.reset_index(drop=False, inplace=True) 
        amort['cod_motivo'] = 'AM'
        amort['motivo'] = 'AMORTIZACION'
        amort.rename(columns={'amortizacion':'importe'}, inplace=True, copy=False)
        df = pd.concat([df, amort], axis=0)
        return df
    
    def ranking_saldo_motivos(self, ejercicio:str = None) -> pd.DataFrame:
        df = self.saldo_motivo_mas_amort()
        if ejercicio == None:
            ejercicio = self.ejercicio
        df = df.loc[df['ejercicio'] == ejercicio]
        df['importe'] = df['importe'].abs()
        df['participacion'] = (df['importe'] / df['importe'].sum()) * 100
        df.sort_values(by='importe', ascending=False, inplace=True)
        return df
    
    def part_motivos_base_otros_ejercicio(self, nro_rank:int = 5) -> pd.DataFrame:
        df = self.saldo_motivo_mas_amort()
        df['importe'] = df['importe'].abs()
        df['participacion'] = df['importe'] / df.groupby('ejercicio')['importe'].transform('sum')
        df['participacion'] = df['participacion'] * 100
        motivos_act = self.ranking_saldo_motivos().head(nro_rank)['cod_motivo'].values.tolist()
        df_motivos = df.loc[df['cod_motivo'].isin(motivos_act)]
        df_otros = df.loc[~df['cod_motivo'].isin(motivos_act)].groupby('ejercicio')[['importe', 'participacion']].sum()
        df_otros.reset_index(drop=False, inplace=True)
        df_otros['cod_motivo'] = 'OT'
        df_otros['motivo'] = 'OTROS MOTIVOS'
        df = pd.concat([df_motivos, df_otros], axis=0) 
        df.sort_values(by=['ejercicio', 'participacion'], ascending=[False, False], inplace=True)
        return df

    def graficar_part_motivos_base_otros_ejercicio(self):
        df = self.part_motivos_base_otros_ejercicio()
        df = df.loc[df['ejercicio'] > '2013']
        df.sort_values(by=['ejercicio', 'participacion'], ascending=[True, True], inplace=True)
        fig = px.bar(
            x=df['ejercicio'], y=df['participacion'], color=df['motivo'],
            title = 'Participación Motios en Variación Saldo Recuperos Cobrar (Base ' + self.ejercicio +')' ,
            labels={'x':'Ejercicio','y':'%'}
        )
        fig.update_layout(showlegend = True)
        fig.show()

    # --------------------------------------------------
    def control_motivo_actualizacion_semestral(self):
        df = self.import_saldo_motivo_actualizacion_semetral(self.ejercicio)
        df = df.loc[df['ejercicio'] == self.ejercicio]
        df['participacion'] = (df['importe'] / df['importe'].sum()) * 100
        df.reset_index(drop=True, inplace=True)
        #Add amortizacion acumulada
        amort = self.import_saldo_barrio_variacion(self.ejercicio).groupby('cod_barrio')[['amortizacion']].sum()
        amort = amort['amortizacion'].abs()
        # amort.reset_index(drop=False, inplace=True)
        df = df.merge(right=amort, on='cod_barrio', copy=False)
        #Add first ejercicio
        first_ejercicio = self.import_saldo_barrio(self.ejercicio)
        first_ejercicio = first_ejercicio.loc[first_ejercicio['saldo_actual'] > 0]
        first_ejercicio.sort_values(by='ejercicio', ascending=True, inplace=True)
        first_ejercicio.drop_duplicates(subset=['cod_barrio'], keep='first', inplace=True)
        first_ejercicio = first_ejercicio.loc[:,['ejercicio', 'cod_barrio']]
        first_ejercicio.rename(columns={'ejercicio':'alta'}, copy=False, inplace=True)
        df = df.merge(right=first_ejercicio, on='cod_barrio', copy=False)
        df.rename(columns={'amortizacion':'amort_acum'}, copy=False, inplace=True)
        df.sort_values(by='participacion', ascending=False, inplace=True)
        ctrl = df.copy()
        ctrl = ctrl.loc[ctrl['alta'] != self.ejercicio]
        ctrl = ctrl.loc[ctrl['amort_acum'] == 0]
        return ctrl

    # --------------------------------------------------
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
            title = 'Diferencia entre saldos finales',
            labels={'x':'Ejercicio','y':'$'}
        )
        fig.update_layout(showlegend = False)
        fig.show()
    
    # --------------------------------------------------
    def graficar_facturado_vs_recaudado(self):
        facturado = self.import_resumen_facturado(self.ejercicio)
        facturado = facturado.groupby('ejercicio')['facturado_total'].sum().to_frame()
        facturado['concepto'] = 'facturado'
        facturado.rename(columns={'facturado_total':'importe'}, inplace=True)
        recaudado = self.import_resumen_recaudado(self.ejercicio)
        recaudado = recaudado.groupby('ejercicio')['recaudado_total'].sum().to_frame()
        recaudado['concepto'] = 'recaudado'
        recaudado.rename(columns={'recaudado_total':'importe'}, inplace=True)
        df = pd.concat([facturado, recaudado], axis=0)
        df.reset_index(drop=False, inplace=True)
        # Add saldo recuperos a cobrar
        saldo = self.import_saldo_barrio(self.ejercicio)
        saldo = saldo.groupby(['ejercicio']).saldo_actual.sum().to_frame()
        saldo.reset_index(drop=False, inplace=True) 
        df = df.merge(saldo, on='ejercicio', copy=False)
        df.reset_index(drop=False, inplace=True)
        df['participacion'] = (df['importe'] / df['saldo_actual']) * 100
        df = df.loc[df['ejercicio'] > '2010']
        fig = px.line(
            x=df['ejercicio'], y=df['participacion'], color=df['concepto'],
            title = 'Facturado VS Recaudado relativos al Total Recuperos a Cobrar (Sist. Recuperos)',
            labels={'x':'Ejercicio','y':'%'}
        )
        fig.update_layout(showlegend = True)
        fig.show()

    # --------------------------------------------------
    def control_recaudado_real_vs_sist_recuperos(self):
        banco = self.import_banco_invico()
        banco = banco.loc[banco['ejercicio'] <= self.ejercicio]
        banco

    # --------------------------------------------------
    def graficar_pend_acreditacion_recaudado(self):
        recaudado = self.import_resumen_recaudado(self.ejercicio)
        recaudado = recaudado.groupby('ejercicio')[['pend_acreditacion', 'recaudado_total']].sum()
        recaudado.reset_index(drop=False, inplace=True)
        df = recaudado
        df['participacion'] = df['pend_acreditacion'].abs() / df['recaudado_total'] *100
        df = recaudado
        df = df.loc[df['ejercicio'] > '2010']
        fig = px.line(
            x=df['ejercicio'], y=df['participacion'],
            title = 'Pendiente de Acreditación en términos relativos al Total Recaudado (Sist. Recuperos)',
            labels={'x':'Ejercicio','y':'%'}
        )
        fig.update_layout(showlegend = False)
        fig.show()

    # --------------------------------------------------
    def graficar_amortizacion_recaudado(self):
        recaudado = self.import_resumen_recaudado(self.ejercicio)
        recaudado = recaudado.groupby('ejercicio')[['amortizacion', 'recaudado_total']].sum()
        recaudado.reset_index(drop=False, inplace=True)
        df = recaudado
        df['participacion'] = df['amortizacion'].abs() / df['recaudado_total'] *100
        df = recaudado
        df = df.loc[df['ejercicio'] > '2010']
        fig = px.line(
            x=df['ejercicio'], y=df['participacion'],
            title = 'Amortización Real en términos relativos al Total Recaudado (Sist. Recuperos)',
            labels={'x':'Ejercicio','y':'%'}
        )
        fig.update_layout(showlegend = False)
        fig.show()


    # --------------------------------------------------
    def graficar_composicion_recaudado_actual(self, nro_rank:int = 5):
        recaudado = self.import_resumen_recaudado(self.ejercicio)
        recaudado = recaudado.groupby('ejercicio')[[
            'amortizacion', 'int_financiero', 'int_mora', 
            'gtos_adm', 'seg_incendio', 'seg_vida', 
            'subsidio', 'pago_amigable', 'escritura',
            'pend_acreditacion'
        ]].sum()
        recaudado.reset_index(drop=False, inplace=True)
        recaudado = recaudado.loc[recaudado['ejercicio'] == self.ejercicio]
        recaudado.drop(columns=['ejercicio'], inplace=True)
        concepto_list=list(recaudado.columns)
        recaudado = pd.melt(
            recaudado, value_vars=concepto_list, var_name='concepto',
            value_name='importe', ignore_index=True
        )
        recaudado['importe'] = recaudado['importe'].abs()
        recaudado['participacion'] = (recaudado['importe'] / recaudado['importe'].sum()) *100
        df = recaudado.copy()
        df.sort_values(by='participacion', ascending=False, inplace=True)
        df = df.head(nro_rank)
        df_otros = pd.DataFrame([{
            'concepto':'otros', 
            'importe': recaudado['importe'].sum() - df['importe'].sum(),
            'participacion': 100 - df['participacion'].sum()
        }])
        df = pd.concat([df, df_otros], axis=0) 
        fig = px.pie(
            values=df['importe'], names=df['concepto'],
            title = 'Composición Recaudado del Ejercicio (Sist. Recuperos)',
        )
        fig.update_layout(showlegend = True)
        fig.show()
