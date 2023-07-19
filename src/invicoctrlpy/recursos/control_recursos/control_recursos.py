#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: SIIF Recursos vs SSCC depósitos
Data required:
    - SIIF rci02
    - SIIF ri102 (no obligatorio)
    - SSCC Consulta General de Movimiento
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
from dataclasses import dataclass

import numpy as np
import pandas as pd
from invicodb.update import update_db
from datar import base, dplyr, f, tidyr

from invicoctrlpy.utils.import_dataframe import ImportDataFrame


@dataclass
# --------------------------------------------------
class ControlRecursos(ImportDataFrame):
    ejercicio:str = None
    input_path:str = None
    db_path:str = None
    update_db:bool = False

    # --------------------------------------------------
    def __post_init__(self):
        if self.db_path == None:
            self.get_db_path()
        if self.update_db:
            self.update_sql_db()
        if self.ejercicio == '':
            self.ejercicio = None
        self.import_dfs()

    # --------------------------------------------------
    def update_sql_db(self):
        if self.input_path == None:
            update_path_input = self.get_update_path_input()
        else:
            update_path_input = self.input_path

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_comprobantes_rec_rci02()
        
        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_banco_invico()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()

    # --------------------------------------------------
    def import_banco_invico(self):
        df = super().import_banco_invico(self.ejercicio)
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
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_rci02(self):
        df = super().import_siif_rci02(self.ejercicio)
        # df = df.loc[df['es_invico'] == False]
        # df = df.loc[df['es_remanente'] == False]
        df = df.loc[df['es_verificado'] == True]
        keep = ['MACRO']
        df['cta_cte'].loc[df.glosa.str.contains('|'.join(keep))] = 'Macro'
        df['grupo'] = np.where(df['cta_cte'] == '10270', 'FONAVI',
                    np.where(df['cta_cte'].isin([
                        "130832-12", "334", "Macro", "Patagonia"]), 'RECUPEROS', 
                        'OTROS'))
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_ri102(self):
        df = super().import_siif_ri102(self.ejercicio)
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def control_mes_grupo(self):
        siif_mes_gpo = self.import_siif_rci02()
        siif_mes_gpo = siif_mes_gpo.loc[siif_mes_gpo['es_invico'] == False]
        siif_mes_gpo = siif_mes_gpo.loc[siif_mes_gpo['es_remanente'] == False]
        siif_mes_gpo = siif_mes_gpo >> \
            dplyr.select(f.mes, f.grupo, f.importe) >> \
            dplyr.group_by(f.mes, f.grupo) >> \
            dplyr.summarise(recursos_siif = base.sum_(f.importe),
                            _groups = 'drop')
        sscc_mes_gpo = self.import_banco_invico()
        sscc_mes_gpo = sscc_mes_gpo >> \
            dplyr.select(
                f.mes, f.grupo, 
                f.importe
            ) >> \
            dplyr.group_by(f.mes, f.grupo) >> \
            dplyr.summarise(
                depositos_sscc = base.sum_(f.importe),
                _groups = 'drop')
        control_mes_gpo = siif_mes_gpo >> \
            dplyr.full_join(sscc_mes_gpo) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            ) >> \
            dplyr.mutate(
                diferencia = f.recursos_siif - f.depositos_sscc
            )
            # dplyr.filter_(~dplyr.near(f.diferencia, 0))
        control_mes_gpo.sort_values(by=['mes', 'grupo'], inplace= True)
        control_mes_gpo = pd.DataFrame(control_mes_gpo)
        control_mes_gpo.reset_index(drop=True, inplace=True)
        return control_mes_gpo

    # --------------------------------------------------
    def control_mes_grupo_cta_cte(self):
        siif_mes_gpo_cta_cte = self.import_siif_rci02()
        siif_mes_gpo_cta_cte = siif_mes_gpo_cta_cte.loc[siif_mes_gpo_cta_cte['es_invico'] == False]
        siif_mes_gpo_cta_cte = siif_mes_gpo_cta_cte.loc[siif_mes_gpo_cta_cte['es_remanente'] == False]
        siif_mes_gpo_cta_cte = siif_mes_gpo_cta_cte >> \
            dplyr.select(f.mes, f.grupo, f.cta_cte, f.importe) >> \
            dplyr.group_by(f.mes, f.grupo, f.cta_cte) >> \
            dplyr.summarise(recursos_siif = base.sum_(f.importe),
                            _groups = 'drop')
        sscc_mes_gpo_cta_cte = self.import_banco_invico()
        sscc_mes_gpo_cta_cte = sscc_mes_gpo_cta_cte >> \
            dplyr.select(
                f.mes, f.grupo, f.cta_cte,
                f.importe
            ) >> \
            dplyr.group_by(f.mes, f.grupo, f.cta_cte) >> \
            dplyr.summarise(
                depositos_sscc = base.sum_(f.importe),
                _groups = 'drop')
        control_mes_gpo_cta_cte = siif_mes_gpo_cta_cte >> \
            dplyr.full_join(sscc_mes_gpo_cta_cte) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            ) >> \
            dplyr.mutate(
                diferencia = f.recursos_siif - f.depositos_sscc
            )
            # dplyr.filter_(~dplyr.near(f.diferencia, 0))
        control_mes_gpo_cta_cte.sort_values(by=['mes', 'grupo', 'cta_cte'], inplace= True)
        control_mes_gpo_cta_cte = pd.DataFrame(control_mes_gpo_cta_cte)
        control_mes_gpo_cta_cte.reset_index(drop=True, inplace=True)
        return control_mes_gpo_cta_cte

    # --------------------------------------------------
    def control_recursos(self):
        group_by = ['ejercicio', 'mes', 'cta_cte', 'grupo']
        siif = self.import_siif_rci02()
        siif = siif.loc[siif['es_invico'] == False]
        siif = siif.loc[siif['es_remanente'] == False]
        siif = siif.groupby(group_by)['importe'].sum()
        siif = siif.reset_index(drop=False)
        siif = siif.rename(columns={'importe':'recursos_siif'})
        sscc = self.import_banco_invico()
        sscc = sscc.groupby(group_by)['importe'].sum()
        sscc = sscc.reset_index(drop=False)
        sscc = sscc.rename(columns={'importe':'depositos_banco'})
        control = pd.merge(siif, sscc, how='outer')       
        return control

    # # --------------------------------------------------
    # def control_completo(self):
    #     icaro_completo = self.icaro_neto_rdeu.copy()
    #     icaro_completo = icaro_completo >> \
    #         dplyr.rename_with(lambda x: 'icaro_' + x) >> \
    #         dplyr.rename(
    #             ejercicio = f.icaro_ejercicio,
    #             mes = f.icaro_mes,
    #             fecha = f.icaro_fecha,
    #             cta_cte = f.icaro_cta_cte,
    #             cuit = f.icaro_cuit,
    #         )
    #     sgf_completo = self.sgf_resumen_rend_cuit.copy()
    #     sgf_completo = sgf_completo >> \
    #         dplyr.rename_with(lambda x: 'sgf_' + x) >> \
    #         dplyr.rename(
    #             ejercicio = f.sgf_ejercicio,
    #             mes = f.sgf_mes,
    #             fecha = f.sgf_fecha,
    #             cta_cte = f.sgf_cta_cte,
    #             cuit = f.sgf_cuit,
    #         )
    #         # dplyr.select(
    #         #     f.mes, f.fecha, f.cta_cte, 
    #         #     f.beneficiario, 
    #         #     libramiento = f.libramiento_sgf,
    #         #     neto_sgf = f.importe_neto, 
    #         #     retenciones_sgf = f.retenciones,
    #         # )
    #     control_completo = sgf_completo >> \
    #         dplyr.full_join(icaro_completo) >> \
    #         dplyr.mutate(
    #             dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
    #         ) >> \
    #         dplyr.mutate(
    #             diferencia = f.icaro_importe - f.sgf_importe_bruto
    #         )
    #     control_completo.sort_values(
    #         by=['mes', 'fecha','cta_cte', 'cuit'], 
    #         inplace= True)
    #     control_completo = pd.DataFrame(control_completo)
    #     control_completo.reset_index(drop=True, inplace=True)
    #     return control_completo

# Ajuste Reemplazo de Chq y Cert Neg???