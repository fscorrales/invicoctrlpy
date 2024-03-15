#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
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
from invicodb.update import update_db

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
        self.import_dfs()

    # --------------------------------------------------
    def update_sql_db(self):
        if self.input_path == None:
            update_path_input = self.get_update_path_input()
        else:
            update_path_input = self.input_path

        update_recuperos = update_db.UpdateSGV(
            update_path_input + '/Gestión Vivienda GV/Sistema Recuperos GV', 
            self.db_path + '/sgv.sqlite')
        update_recuperos.update_barrios_nuevos()
        update_recuperos.update_resumen_facturado()
        update_recuperos.update_resumen_recaudado()
        update_recuperos.update_saldo_barrio_variacion()
        update_recuperos.update_saldo_barrio()
        update_recuperos.update_saldo_motivo_por_barrio()
        update_recuperos.update_saldo_motivo()
        update_recuperos.update_saldo_recuperos_cobrar_variacion()
        
        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_banco_invico()


    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()

    # --------------------------------------------------
    def importBarriosNuevos(self):
        df = super().import_barrios_nuevos(
            ejercicio = self.ejercicio
        )
        return df

    # --------------------------------------------------
    def importResumenFacturado(self):
        df = super().import_resumen_facturado(
            ejercicio = self.ejercicio
        )
        return df

    # --------------------------------------------------
    def importResumenRecaudado(self):
        df = super().import_resumen_recaudado(
            ejercicio = self.ejercicio
        )
        return df

    # --------------------------------------------------
    def importSaldoBarrioVariacion(self):
        df = super().import_saldo_barrio_variacion(
            ejercicio = self.ejercicio
        )
        return df

    # --------------------------------------------------
    def importSaldoBarrio(self):
        df = super().import_saldo_barrio(
            ejercicio = self.ejercicio
        )
        return df

    # --------------------------------------------------
    def importSaldoRecuperosCobrarVariacion(self):
        df = super().import_saldo_recuperos_cobrar_variacion(
            ejercicio = self.ejercicio
        )
        return df

    # --------------------------------------------------
    def importSaldoMotivoPorBarrio(self):
        df = super().import_saldo_motivo_por_barrio(
            ejercicio = self.ejercicio
        )
        return df

    # --------------------------------------------------
    def importSaldoMotivo(self):
        df = super().import_saldo_motivo(
            ejercicio = self.ejercicio
        )
        return df

    # --------------------------------------------------
    def import_banco_invico(self, ejercicio):
        df = super().import_banco_invico(ejercicio)
        df = df.loc[df['movimiento'] == 'DEPOSITO']
        dep_transf_int = ['034', '004']
        dep_pf = ['214', '215']
        dep_otros = ['003', '055', '005', '013']
        dep_cert_neg = ['18']
        df = df.loc[~df['cod_imputacion'].isin(
            dep_transf_int + dep_pf + dep_otros + dep_cert_neg
            )]
        df['grupo'] = np.where(df['cta_cte'] == '10270', 'FONAVI',
                        np.where(df['cta_cte'].isin([
                            "130832-12", "334", "Macro", "Patagonia"]), 'RECUPEROS', 
                            'OTROS'))
        df = df.loc[df['grupo'] == 'RECUPEROS']
        df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def controlSaldosRecuperosCobrarVariacion(self):
        df = self.importSaldoRecuperosCobrarVariacion()
        df = df.loc[df['concepto'].isin(['SALDO AL INICIO:', 'SALDO AL FINAL:'])]
        return df

    def graficarSaldosBarrio(self):
        df = self.importSaldoBarrio()
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
    def controlSumaSaldoBarrioVariacion(self):
        df = self.importSaldoBarrioVariacion()
        df = df.groupby(["ejercicio"])[["saldo_inicial", "amortizacion", "cambios", "saldo_final"]].sum()
        df["suma_algebraica"] = df.saldo_inicial + df.amortizacion + df.cambios
        # df["dif_saldo_final"] = df.saldo_final - df.suma_algebraica
        df['%_saldo_explicado'] = ((df.amortizacion + df.cambios) / (df.saldo_final - df.saldo_inicial)) * 100
        return df

    # --------------------------------------------------
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
    def saldoMotivoMasAmort(self) -> pd.DataFrame:
        df = self.importSaldoMotivo()
        # amort = self.control_suma_saldo_barrio_variacion()
        # amort.reset_index(drop=False, inplace=True)
        # amort = amort.loc[:, ['ejercicio', 'amortizacion']]
        # amort['cod_motivo'] = '-'
        # amort['motivo'] = 'AMORTIZACION'
        # amort.rename(columns={'amortizacion':'importe'}, inplace=True, copy=False)
        amort = self.importResumenRecaudado()
        amort = amort.groupby(["ejercicio"])[["amortizacion"]].sum()
        amort.reset_index(drop=False, inplace=True) 
        amort['cod_motivo'] = 'AM'
        amort['motivo'] = 'AMORTIZACION'
        amort.rename(columns={'amortizacion':'importe'}, inplace=True, copy=False)
        df = pd.concat([df, amort], axis=0)
        return df
    
    def rankingSaldoMotivos(self, ejercicio:str = None) -> pd.DataFrame:
        df = self.saldoMotivoMasAmort()
        if ejercicio == None:
            ejercicio = self.ejercicio
        df = df.loc[df['ejercicio'] == ejercicio]
        df['importe'] = df['importe'].abs()
        df['participacion'] = (df['importe'] / df['importe'].sum()) * 100
        df.sort_values(by='importe', ascending=False, inplace=True)
        return df
    
    def partMotivosBaseOtrosEjercicio(self, nro_rank:int = 5) -> pd.DataFrame:
        df = self.saldoMotivoMasAmort()
        df['importe'] = df['importe'].abs()
        df['participacion'] = df['importe'] / df.groupby('ejercicio')['importe'].transform('sum')
        df['participacion'] = df['participacion'] * 100
        motivos_act = self.rankingSaldoMotivos().head(nro_rank)['cod_motivo'].values.tolist()
        df_motivos = df.loc[df['cod_motivo'].isin(motivos_act)]
        df_otros = df.loc[~df['cod_motivo'].isin(motivos_act)].groupby('ejercicio')[['importe', 'participacion']].sum()
        df_otros.reset_index(drop=False, inplace=True)
        df_otros['cod_motivo'] = 'OT'
        df_otros['motivo'] = 'OTROS MOTIVOS'
        df = pd.concat([df_motivos, df_otros], axis=0) 
        df.sort_values(by=['ejercicio', 'participacion'], ascending=[False, False], inplace=True)
        return df

    def graficarPartMotivosBaseOtrosEjercicio(self):
        df = self.partMotivosBaseOtrosEjercicio()
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
    def barriosNuevosVsEntregaDeViviendas(self):
        barrios_nuevos = self.importBarriosNuevos()
        barrios_nuevos = barrios_nuevos.groupby(['ejercicio'])[['importe_total']].sum()
        barrios_nuevos = barrios_nuevos.reset_index(drop=False)
        barrios_nuevos = barrios_nuevos.rename(columns={'importe_total':'barrios_nuevos'}, copy=False)
        entrega_viviendas = self.importSaldoMotivo()
        entrega_viviendas = entrega_viviendas.loc[entrega_viviendas['cod_motivo'] == '1']
        entrega_viviendas = entrega_viviendas.loc[:, ['ejercicio', 'importe']]
        entrega_viviendas = entrega_viviendas.rename(columns={'importe':'entrega_viviendas'}, copy=False)
        df = pd.merge(barrios_nuevos, entrega_viviendas, on='ejercicio')
        df = df.reset_index(drop=True)
        df['diferencia'] = df.barrios_nuevos - df.entrega_viviendas
        return df

    # --------------------------------------------------
    def barriosNuevosVsEntregaDeViviendasPorBarrio(self):
        barrios_nuevos = self.importBarriosNuevos()
        barrios_nuevos = barrios_nuevos.groupby(['cod_barrio'])[['importe_total']].sum()
        barrios_nuevos = barrios_nuevos.reset_index(drop=False)
        barrios_nuevos = barrios_nuevos.rename(columns={'importe_total':'barrios_nuevos'}, copy=False)
        entrega_viviendas = self.importSaldoMotivoPorBarrio()
        entrega_viviendas = entrega_viviendas.loc[entrega_viviendas['cod_motivo'] == '001']
        entrega_viviendas = entrega_viviendas.groupby(['cod_barrio'])[['importe']].sum()
        entrega_viviendas = entrega_viviendas.reset_index(drop=False)
        entrega_viviendas = entrega_viviendas.rename(columns={'importe':'entrega_viviendas'}, copy=False)
        # Obtener los índices faltantes en barrios_nuevos
        barrios_nuevos = barrios_nuevos.set_index('cod_barrio')
        entrega_viviendas = entrega_viviendas.set_index('cod_barrio')
        missing_indices = barrios_nuevos.index.difference(entrega_viviendas.index)
        # Reindexar el DataFrame siif con los índices barrios_nuevos
        barrios_nuevos = barrios_nuevos.reindex(barrios_nuevos.index.union(missing_indices))
        entrega_viviendas = entrega_viviendas.reindex(barrios_nuevos.index)
        barrios_nuevos = barrios_nuevos.fillna(0)
        entrega_viviendas = entrega_viviendas.fillna(0)        
        df = pd.merge(barrios_nuevos, entrega_viviendas, on='cod_barrio')
        df = df.reset_index()
        df['diferencia_abs'] = abs(df.barrios_nuevos - df.entrega_viviendas)
        df = df.loc[(df['diferencia_abs'] > 0.05)]
        df = df.sort_values(by='diferencia_abs', ascending=False)
        return df
    
    # --------------------------------------------------
    def recaudadoSistRecuperosVsRecaudadoReal(self):
        recaudado_recuperos = self.importResumenRecaudado()
        recaudado_recuperos = recaudado_recuperos.loc[recaudado_recuperos['ejercicio'].astype(int) > 2017]
        ejercicios = recaudado_recuperos['ejercicio'].unique().tolist()
        recaudado_recuperos = recaudado_recuperos.groupby(['ejercicio'])[['recaudado_total']].sum()
        recaudado_recuperos = recaudado_recuperos.reset_index(drop=False)
        recaudado_recuperos = recaudado_recuperos.rename(columns={'recaudado_total':'recaudado_recuperos'}, copy=False)
        recaudado_banco = self.import_banco_invico(ejercicio = ejercicios)
        recaudado_banco = recaudado_banco.loc[:, ['ejercicio', 'importe']]
        recaudado_banco = recaudado_banco.groupby(['ejercicio'])[['importe']].sum()
        recaudado_banco = recaudado_banco.reset_index(drop=False)
        recaudado_banco = recaudado_banco.rename(columns={'importe':'recaudado_banco'}, copy=False)
        df = pd.merge(recaudado_recuperos, recaudado_banco, on='ejercicio')
        df = df.reset_index(drop=True)
        df['diferencia'] = df.recaudado_recuperos - df.recaudado_banco
        df['dif%'] = (df.diferencia / df.recaudado_recuperos) * 100
        return df

    # --------------------------------------------------
    def recaudadoSistRecuperosVsRecaudadoReal(self, group_by_month:bool = False):
        recaudado_recuperos = self.importResumenRecaudado()
        recaudado_recuperos = recaudado_recuperos.loc[recaudado_recuperos['ejercicio'].astype(int) > 2017]
        ejercicios = recaudado_recuperos['ejercicio'].unique().tolist()
        recaudado_banco = self.import_banco_invico(ejercicio = ejercicios)

        if group_by_month:
            group_by = 'mes'
            recaudado_recuperos['mes'] = recaudado_recuperos['mes'].str[:2]
            recaudado_banco['mes'] = recaudado_banco['mes'].str[:2]
        else:
            group_by = 'ejercicio'
        recaudado_recuperos = recaudado_recuperos.groupby([group_by])[['recaudado_total']].sum()
        recaudado_recuperos = recaudado_recuperos.reset_index(drop=False)
        recaudado_recuperos = recaudado_recuperos.rename(columns={'recaudado_total':'recaudado_recuperos'}, copy=False)
        recaudado_banco = recaudado_banco.loc[:, [group_by, 'importe']]
        recaudado_banco = recaudado_banco.groupby([group_by])[['importe']].sum()
        recaudado_banco = recaudado_banco.rename(columns={'importe':'recaudado_banco'}, copy=False)
        recaudado_banco = recaudado_banco.reset_index(drop=False)
        df = pd.merge(recaudado_recuperos, recaudado_banco, on=group_by)
        df = df.reset_index(drop=True)
        df['diferencia'] = df.recaudado_recuperos - df.recaudado_banco
        df['dif%'] = (df.diferencia / df.recaudado_recuperos) * 100
        return df

    # --------------------------------------------------
    def graficarRecaudadoSistRecuperosVsRecaudadoReal(self, group_by_month:bool = False):
        df = self.recaudadoSistRecuperosVsRecaudadoReal(group_by_month = group_by_month)
        if group_by_month:
            group_by = 'mes'
        else:
            group_by = 'ejercicio'
        fig = px.line(
            x=df[group_by], y=df['dif%'],
            title = 'Diferencia Sist. Recuperos VS Banco Real como porcentaje del Sist. Recuperos',
            labels={'x':group_by,'y':'%'}
        )
        fig.update_layout(showlegend = False)
        fig.show()

    # --------------------------------------------------
    def graficarFacturadoVsRecaudado(self):
        facturado = self.importResumenFacturado()
        facturado = facturado.groupby('ejercicio')['facturado_total'].sum().to_frame()
        facturado['concepto'] = 'facturado'
        facturado.rename(columns={'facturado_total':'importe'}, inplace=True)
        recaudado = self.importResumenRecaudado()
        recaudado = recaudado.groupby('ejercicio')['recaudado_total'].sum().to_frame()
        recaudado['concepto'] = 'recaudado'
        recaudado.rename(columns={'recaudado_total':'importe'}, inplace=True)
        df = pd.concat([facturado, recaudado], axis=0)
        df.reset_index(drop=False, inplace=True)
        # Add saldo recuperos a cobrar
        saldo = self.importSaldoBarrio()
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
    def graficarPendAcreditacionRecaudado(self):
        recaudado = self.importResumenRecaudado()
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
    def graficarAmortizacionRecaudado(self):
        recaudado = self.importResumenRecaudado()
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
    def graficarComposicionRecaudadoActual(self, nro_rank:int = 5):
        recaudado = self.importResumenRecaudado()
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

    # --------------------------------------------------
    def altaBarriosSinAmort(self):
        df = self.importSaldoBarrioVariacion()
        # Nos limitamos a aquellos barrios que aún tienen saldo
        df = df.loc[df['saldo_final'] > 0]
        # Nos limitamos a aquellos barrios dados de alta hasta el ejercicio anterior
        df_ant = df.copy()
        df_ant = df_ant.loc[df_ant['ejercicio'].astype(int) < int(self.ejercicio)]
        df_ant = df_ant.groupby('barrio').amortizacion.sum().to_frame()
        df_ant = df_ant.reset_index(drop=False)
        df_actual = df.loc[df['ejercicio'] == self.ejercicio].copy()
        df_actual = df_actual.drop(columns=['amortizacion'])
        df_actual = df_actual.merge(df_ant, on='barrio', copy=False)
        df_actual['amortizacion'] = df_actual['amortizacion'] * (-1)
        df_actual = df_actual.loc[df_actual['amortizacion'] < 0.01]
        return df_actual

    # --------------------------------------------------
    def graficarAltaBarriosSinAmort(self, base_saldo:bool = False):
        df_actual = self.importSaldoBarrioVariacion()
        df_actual = df_actual.loc[df_actual['ejercicio'] == self.ejercicio]
        df_alta = self.altaBarriosSinAmort()
        if base_saldo:
            importe = [
                sum(df_alta['saldo_final']), 
                sum(df_actual['saldo_final']) - sum(df_alta['saldo_final'])
            ]
            title = 'Saldo Final de Barrios con y sin Amortizaciones (Sist. Recuperos)'
        else:
            importe = [len(df_alta), len(df_actual) - len(df_alta)]
            title = 'Cantidad de Barrios con y sin Amortizaciones (Sist. Recuperos)'
        df = {
            'concepto': ['Barrios sin amortizaciones', 'Barrios con amortizaciones'],
            'importe': importe,
        }
        df = pd.DataFrame(df)
        fig = px.pie(
            values=df['importe'], names=df['concepto'],
            title = title,
        )
        fig.update_layout(showlegend = True)
        fig.show()