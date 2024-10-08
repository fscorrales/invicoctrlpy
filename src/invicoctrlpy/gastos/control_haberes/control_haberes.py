#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control ejecución partida 100 SIIF (haberes)
Data required:
    - SIIF rcg01_uejp
    - SIIF gto_rpa03g
    - SIIF rcocc31 (2122-1-2)
    - SIIF rdeu
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
from dataclasses import dataclass

import pandas as pd
from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicodb.update import update_db


@dataclass
# --------------------------------------------------
class ControlHaberes(ImportDataFrame):
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

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_comprobantes_gtos_rcg01_uejp()
        update_siif.update_comprobantes_gtos_gpo_part_gto_rpa03g()
        update_siif.update_mayor_contable_rcocc31()
        update_siif.update_deuda_flotante_rdeu012()
        
        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_banco_invico()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.import_siif_comprobantes_haberes_neto_rdeu(
            self.ejercicio, neto_art=True, neto_gcias_310=True)
        self.import_siif_rdeu012()
        self.import_banco_invico()

    # --------------------------------------------------
    def import_siif_rdeu012(self):
        df = super().import_siif_rdeu012()
        cyo_rdeu = self.siif_comprobantes_haberes['nro_comprobante'].unique()
        df = df.loc[df['nro_comprobante'].isin(cyo_rdeu)]
        self.siif_rdeu012 = df
        return self.siif_rdeu012

    # --------------------------------------------------
    def import_banco_invico(self):
        df = super().import_banco_invico(self.ejercicio)
        df = df.loc[df['movimiento'] != 'DEPOSITO']
        df = df.loc[df['cta_cte'] == '130832-04']
        keep = ['GCIAS', 'GANANCIAS']
        df = df.loc[~df.concepto.str.contains('|'.join(keep))]
        df['importe'] = df['importe'] * (-1)
        dep_transf_int = ['034', '004']
        dep_otros = ['003', '055', '005', '013']
        df = df.loc[~df['cod_imputacion'].isin(
            dep_transf_int + dep_otros
            )]
        df.reset_index(drop=True, inplace=True)
        self.sscc_banco_invico = df
        return self.sscc_banco_invico

    # --------------------------------------------------
    def control_cruzado(self, groupby_cols:list = ['ejercicio', 'mes']):
        siif = self.siif_comprobantes_haberes_neto_rdeu.copy()
        siif = siif.loc[:, groupby_cols + ['importe']]
        siif = siif.groupby(groupby_cols)['importe'].sum()
        siif = siif.reset_index()
        siif = siif.rename(columns={'importe':'ejecutado_siif'})
        # siif = siif >> \
        #     dplyr.select(f.mes, f.importe) >> \
        #     dplyr.group_by(f.mes) >> \
        #     dplyr.summarise(ejecutado_siif = base.sum_(f.importe),
        #                     _groups = 'drop')
        sscc = self.sscc_banco_invico.copy()
        sscc = sscc.loc[:, groupby_cols + ['importe']]
        sscc = sscc.groupby(groupby_cols)['importe'].sum()
        sscc = sscc.reset_index()
        sscc = sscc.rename(columns={'importe':'pagado_sscc'})
        # sscc = sscc >> \
        #     dplyr.select(
        #         f.mes, f.importe
        #     ) >> \
        #     dplyr.group_by(f.mes) >> \
        #     dplyr.summarise(
        #         pagado_sscc = base.sum_(f.importe),
        #         _groups = 'drop')
        df = pd.merge(siif, sscc, how='outer', on=groupby_cols, copy=False)
        df[['ejecutado_siif', 'pagado_sscc']] = df[['ejecutado_siif', 'pagado_sscc']].fillna(0)
        df['diferencia'] = df.ejecutado_siif - df.pagado_sscc
        # df = siif >> \
        #     dplyr.full_join(sscc) >> \
        #     dplyr.mutate(
        #         dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
        #     ) >> \
        #     dplyr.mutate(
        #         diferencia = f.ejecutado_siif- f.pagado_sscc
        #     )
        #     dplyr.filter_(~dplyr.near(f.diferencia, 0))
        df = df.sort_values(by=['ejercicio','mes'])
        df = pd.DataFrame(df)
        df['dif_acum'] = df['diferencia'].cumsum()
        df.reset_index(drop=True, inplace=True)
        return df
