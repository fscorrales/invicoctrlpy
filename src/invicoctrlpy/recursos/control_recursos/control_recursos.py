#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
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

__all__ = ['ControlRecursos']

from dataclasses import dataclass

import numpy as np
import pandas as pd
# from invicodb.update import update_db
from typing import List

from invicoctrlpy.utils.import_dataframe import ImportDataFrame


@dataclass
# --------------------------------------------------
class ControlRecursos(ImportDataFrame):
    ejercicio:str = None
    input_path:str = None
    db_path:str = None
    # update_db:bool = False

    # --------------------------------------------------
    def __post_init__(self):
        if self.db_path == None:
            self.get_db_path()
        # if self.update_db:
        #     self.update_sql_db()
        if self.ejercicio == '':
            self.ejercicio = None
        self.import_dfs()

    # --------------------------------------------------
    # def update_sql_db(self):
    #     if self.input_path == None:
    #         update_path_input = self.get_update_path_input()
    #     else:
    #         update_path_input = self.input_path

    #     update_siif = update_db.UpdateSIIF(
    #         update_path_input + '/Reportes SIIF', 
    #         self.db_path + '/siif.sqlite')
    #     update_siif.update_comprobantes_rec_rci02()
        
    #     update_sscc = update_db.UpdateSSCC(
    #         update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
    #         self.db_path + '/sscc.sqlite')
    #     update_sscc.update_ctas_ctes()
    #     update_sscc.update_banco_invico()

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
    def control_mes_grupo(self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']):
        siif_mes_gpo = self.import_siif_rci02()
        siif_mes_gpo = siif_mes_gpo.loc[siif_mes_gpo['es_invico'] == False]
        siif_mes_gpo = siif_mes_gpo.loc[siif_mes_gpo['es_remanente'] == False]
        
        siif_mes_gpo = siif_mes_gpo.loc[:, groupby_cols + ['importe']]
        siif_mes_gpo = siif_mes_gpo.groupby(groupby_cols).sum(numeric_only=True)
        siif_mes_gpo = siif_mes_gpo.reset_index()
        siif_mes_gpo = siif_mes_gpo.rename(columns={'importe': 'recursos_siif'})
        sscc_mes_gpo = self.import_banco_invico()
        sscc_mes_gpo = sscc_mes_gpo.loc[:, groupby_cols + ['importe']]
        sscc_mes_gpo = sscc_mes_gpo.groupby(groupby_cols).sum(numeric_only=True)
        sscc_mes_gpo = sscc_mes_gpo.reset_index()
        sscc_mes_gpo = sscc_mes_gpo.rename(columns={'importe': 'depositos_sscc'})
        siif_mes_gpo = siif_mes_gpo.set_index(groupby_cols)
        sscc_mes_gpo = sscc_mes_gpo.set_index(groupby_cols)
        # Obtener los índices faltantes en icaro
        missing_indices = siif_mes_gpo.index.difference(sscc_mes_gpo.index)
        # Reindexar el DataFrame icaro con los índices faltantes
        sscc_mes_gpo = sscc_mes_gpo.reindex(sscc_mes_gpo.index.union(missing_indices))
        siif_mes_gpo = siif_mes_gpo.reindex(sscc_mes_gpo.index)
        siif_mes_gpo = siif_mes_gpo.fillna(0)
        sscc_mes_gpo = sscc_mes_gpo.fillna(0)
        control_mes_gpo = pd.merge(siif_mes_gpo, sscc_mes_gpo, how='inner', on=groupby_cols)
        control_mes_gpo['diferencia'] = control_mes_gpo['recursos_siif'] - control_mes_gpo['depositos_sscc']

        control_mes_gpo = control_mes_gpo.sort_values(by=groupby_cols)

        control_mes_gpo = control_mes_gpo.reset_index()
        return control_mes_gpo
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
