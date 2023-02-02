#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: Cálculo de Remanente y Mal Llamado Remanente
Data required:
    - SIIF rf602
    - SIIF rf610
    - SIIF rci02
    - SIIF rdeu012
    - SIIF rdeu012bc_c (Pedir a Tesorería General de la Provincia)
    - SSCC ctas_ctes (manual data)
    - Saldos por cuenta Banco INVICO (SSCC) al 
    31/12 de cada año (SSCC-Cuentas-Resumen Gral de Saldos)
    - Icaro (¿sólo para fondos de reparo? ¿Vale la pena?)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
from dataclasses import dataclass, field

import pandas as pd
import update_db
from datar import base, dplyr, f, tidyr
import os

from invicoctrlpy.utils.import_dataframe import ImportDataFrame


@dataclass
# --------------------------------------------------
class Remamente(ImportDataFrame):
    ejercicio:str = str(dt.datetime.now().year)
    db_path:str = None
    update_db:bool = False
    siif_desc_pres: pd.DataFrame = field(init=False, repr=False)
    
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

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_ppto_gtos_fte_rf602()
        update_siif.update_ppto_gtos_desc_rf610()
        update_siif.update_comprobantes_rec_rci02()
        update_siif.update_deuda_flotante_rdeu012()
        update_siif.update_deuda_flotante_rdeu012b2_c()

        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_sdo_final_banco_invico()

        # update_icaro = update_db.UpdateIcaro(
        #     self.get_outside_path() + '/R Output/SQLite Files/ICARO.sqlite', 
        #     self.db_path + '/icaro.sqlite')
        # update_icaro.migrate_icaro()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.siif_desc_pres = self.import_siif_desc_pres(ejercicio_to=self.ejercicio)

    # --------------------------------------------------
    def import_sdo_final_banco_invico(self) -> pd.DataFrame:
        return super().import_sdo_final_banco_invico(ejercicio = self.ejercicio)

    # --------------------------------------------------
    def import_siif_rdeu012b2_c(self) -> pd.DataFrame:
        return super().import_siif_rdeu012b2_c(mes_hasta = '12/' + self.ejercicio)

    # --------------------------------------------------
    def import_siif_ppto_gto_con_desc(self) -> pd.DataFrame:
        df = super().import_siif_ppto_gto_con_desc(ejercicio = self.ejercicio)
        df = df.loc[df['partida'].isin(['421', '422'])]
        df = df.loc[df['fuente'] != '11']
        df.drop([
            'grupo', 'credito_original', 'comprometido', 
            'pendiente', 'desc_part', 'desc_gpo'
        ], axis=1, inplace=True)
        df['remte'] = 0
        df['mal_remte'] = 0
        df['saldo_remte'] = df['saldo']
        df['cc_remte'] = None
        df['cc_mal_remte'] = None
        return df

    # --------------------------------------------------
    def remanente_met_1(self):
        SALDO_UCAPFI = 4262062.77
        banco_sscc = self.import_sdo_final_banco_invico()
        rdeu = self.import_siif_rdeu012(ejercicio=self.ejercicio)
        rdeu_max_mes = rdeu.loc[rdeu['fecha'] == max(rdeu['fecha'])].drop_duplicates(subset='mes_hasta')
        rdeu = rdeu.loc[rdeu['mes_hasta'] == rdeu_max_mes.iloc[0]['mes_hasta']]
        rem_met_1 = {
            "Fuente 10 y 12":
                {"saldo_bco":banco_sscc.loc[banco_sscc['cta_cte'].isin(["130832-03", "130832-06"])]["saldo"].sum(),
                "rdeu":rdeu.loc[rdeu.fuente.isin(["10", "12"])]["saldo"].sum()},
            "Fuente 13":
                {"saldo_bco":banco_sscc.loc[banco_sscc['cta_cte'] == "130832-16"]["saldo"].sum() + SALDO_UCAPFI,
                "rdeu":rdeu.loc[rdeu.fuente.isin(["13"])]["saldo"].sum()
                },
            "Fuente 11":
                {"saldo_bco":banco_sscc.loc[~banco_sscc['cta_cte'].isin(
                    ["130832-03", "130832-06", "130832-16"])]["saldo"].sum() - SALDO_UCAPFI,
                "rdeu":rdeu.loc[rdeu.fuente.isin(["11"])]["saldo"].sum()
                }        
            }
        rem_met_1 = pd.DataFrame.from_dict(rem_met_1, orient="index")
        rem_met_1.reset_index(inplace=True)
        rem_met_1.columns = ['fuente', 'saldo_bco', 'rdeu']
        rem_met_1["rte_met_1"] = rem_met_1.saldo_bco - rem_met_1.rdeu
        return rem_met_1

    # --------------------------------------------------
    def remanente_met_2(self):
        recursos = self.import_siif_rci02(ejercicio=self.ejercicio)
        gastos = self.import_siif_rf602(ejercicio=self.ejercicio)
        rem_met_2 = recursos.importe.groupby([recursos.fuente]).sum()
        rem_met_2 = pd.concat([rem_met_2, gastos.ordenado.groupby([gastos.fuente]).sum(),
        gastos.saldo.groupby([gastos.fuente]).sum()], axis=1)
        rem_met_2.columns = ["recursos", "gastos", "saldo_pres"]
        rem_met_2["rte_met_2"] = rem_met_2.recursos - rem_met_2.gastos
        rem_met_2.reset_index(inplace=True)
        # rem_met_2 = rem_met_2[~rem_met_2.index.isin(['11'], level=1)]
        # rem_met_2.dropna(inplace=True)
        return rem_met_2

    # --------------------------------------------------
    def remanente_met_2_hist(self):
        recursos = self.import_siif_rci02()
        gastos = self.import_siif_rf602()
        rem_solicitado = recursos.loc[recursos.es_remanente == True].importe.groupby([recursos.ejercicio, recursos.fuente]).sum().to_frame()
        rem_met_2_hist = recursos.importe.groupby([recursos.ejercicio, recursos.fuente]).sum()
        rem_met_2_hist = pd.concat([rem_met_2_hist, gastos.ordenado.groupby([gastos.ejercicio, gastos.fuente]).sum(),
        gastos.saldo.groupby([gastos.ejercicio, gastos.fuente]).sum(), rem_solicitado.groupby(level=[1])['importe'].shift(-1)], axis=1)
        rem_met_2_hist.columns = ["recursos", "gastos", "saldo_pres", "rte_solicitado"]
        rem_met_2_hist["rte_met_2"] = rem_met_2_hist.recursos - rem_met_2_hist.gastos
        # rem_met_2_hist["mal_rte_met_2"] = rem_met_2_hist.saldo_pres- rem_met_2_hist.rte_met_2
        rem_met_2_hist["dif_rte_solicitado"] = rem_met_2_hist.rte_solicitado - rem_met_2_hist.rte_met_2
        rem_met_2_hist = rem_met_2_hist[~rem_met_2_hist.index.isin(['11'], level=1)]
        #No sé qué pasó en Fuente 13 en el 2013, por eso lo filtro
        rem_met_2_hist = rem_met_2_hist[~rem_met_2_hist.index.isin(['2011', '2012', '2013'], level=0)]
        rem_met_2_hist.reset_index(inplace=True)
        rem_met_2_hist.dropna(inplace=True)
        return rem_met_2_hist

    # --------------------------------------------------
    def remanente_dif_met(self):
        rem_met_1 = self.remanente_met_1()
        rem_met_1 = rem_met_1.loc[:, ['fuente', 'rte_met_1']]
        rem_met_2 = self.remanente_met_2()
        rem_met_2 = rem_met_2.loc[:, ['fuente', 'rte_met_2']]
        # rem_met_2 = rem_met_2.loc[~rem_met_2['fuente'].isin(['10', '12'])]
        rem_met_2 = pd.DataFrame([
            ['Fuente 10 y 12', rem_met_2.loc[rem_met_2['fuente'].isin(['10', '12'])]['rte_met_2'].sum()],
            ['Fuente 11', rem_met_2.loc[rem_met_2['fuente'] == '11']['rte_met_2'].sum()],
            ['Fuente 13', rem_met_2.loc[rem_met_2['fuente'] == '13']['rte_met_2'].sum()]
        ], columns=['fuente', 'rte_met_2'])
        rem_met = rem_met_1.merge(rem_met_2, how='left', on='fuente')
        rem_met['dif_metodos'] = rem_met.rte_met_1 - rem_met.rte_met_2
        return rem_met