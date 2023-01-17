#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: Control ejecución partida 100 SIIF (haberes)
Data required:
    - Slave
    - SIIF rcg01_uejp
    - SIIF gto_rpa03g
    - SGF Resumen Rendicions por Proveedor
    - SSCC Resumen General de Movimientos (para agregar dep. emb. x alim. 130832-05)
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
from dataclasses import dataclass

import pandas as pd
import update_db
from datar import base, dplyr, f, tidyr

from invicoctrlpy.utils.import_dataframe import ImportDataFrame


@dataclass
# --------------------------------------------------
class ControlHonorarios(ImportDataFrame):
    ejercicio:str = str(dt.datetime.now().year)
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
        update_path_input = self.get_update_path_input()

        update_slave = update_db.UpdateSlave(
            update_path_input + '/Slave/Slave.mdb', 
            self.db_path + '/slave.sqlite')
        # update_slave.migrate_slave()

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_comprobantes_gtos_rcg01_uejp()
        update_siif.update_comprobantes_gtos_gpo_part_gto_rpa03g()

        update_sgf = update_db.UpdateSGF(
            update_path_input + '/Sistema Gestion Financiera', 
            self.db_path + '/sgf.sqlite')
        update_sgf.update_resumen_rend_prov()
        
        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_banco_invico()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.import_slave()
        self.import_resumen_rend_honorarios(self.ejercicio, True)
        self.import_siif_comprobantes()

    # --------------------------------------------------
    def import_slave(self):
        df = super().import_slave(ejercicio = self.ejercicio)
        cta_cte = self.import_siif_rcg01_uejp(self.ejercicio)
        cta_cte = cta_cte.loc[:, ['nro_comprobante', 'cta_cte']]
        df = df.merge(cta_cte, on='nro_comprobante', how='left')
        # df['origen'] = df['cta_cte'].apply(lambda x: 'FUNC' if x == '130832-05' else 'EPAM')
        self.slave = df
        return self.slave

    # --------------------------------------------------
    def import_siif_comprobantes(self):
        df = super().siif_comprobantes(ejercicio = self.ejercicio)
        df = df.loc[df['cuit'] == '30632351514']
        df = df.loc[df['grupo'] == '300']
        df = df.loc[df['cta_cte'].isin(['130832-05', '130832-07'])]
        keep = ['HONOR', 'RECON', 'LOC']
        df = df.loc[~df.glosa.str.contains('|'.join(keep))]
        # df['origen'] = df['cta_cte'].apply(lambda x: 'FUNC' if x == '130832-05' else 'EPAM')
        self.siif_comprobantes = df
        return self.siif_comprobantes

    # # --------------------------------------------------
    # def slave_vs_siif(self):
    #     slave = self.slave.copy()
    #     slave = slave >> \
    #         dplyr.select(
    #             f.mes, f.nro_comprobante, 
    #             f.cta_cte, f.importe_bruto) >> \
    #         dplyr.group_by(f.mes, f.nro_comprobante, f.cta_cte) >> \
    #         dplyr.summarise(ejecutado_slave = base.sum_(f.importe_bruto),
    #                         _groups = 'drop')
    #     siif = self.siif_comprobantes.copy()
    #     siif = siif >> \
    #         dplyr.select(
    #             f.mes, f.nro_comprobante, 
    #             f.cta_cte, f.importe
    #         ) >> \
    #         dplyr.group_by(f.mes, f.nro_comprobante, f.cta_cte) >> \
    #         dplyr.summarise(
    #             ejecutado_siif = base.sum_(f.importe),
    #             _groups = 'drop')
    #     control_mes = slave >> \
    #         dplyr.full_join(siif) >> \
    #         dplyr.mutate(
    #             dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
    #         ) >> \
    #         dplyr.mutate(
    #             diferencia = f.ejecutado_slave - f.ejecutado_siif
    #         )
    #     #     dplyr.filter_(~dplyr.near(f.diferencia, 0))
    #     control_mes.sort_values(by=['mes', 'cta_cte'], inplace= True)
    #     control_mes = pd.DataFrame(control_mes)
    #     control_mes['dif_acum'] = control_mes['diferencia'].cumsum()
    #     control_mes.reset_index(drop=True, inplace=True)
    #     return control_mes

    # --------------------------------------------------
    def control_mes(self):
        slave_mes = self.slave.copy()
        slave_mes = slave_mes >> \
            dplyr.select(f.mes, f.cta_cte, f.importe_bruto) >> \
            dplyr.group_by(f.mes, f.cta_cte) >> \
            dplyr.summarise(ejecutado_slave = base.sum_(f.importe_bruto),
                            _groups = 'drop')
        sgf_mes = self.sgf_resumen_rend_honorarios.copy()
        sgf_mes = sgf_mes >> \
            dplyr.select(
                f.mes, f.cta_cte, f.importe_bruto
            ) >> \
            dplyr.group_by(f.mes, f.cta_cte) >> \
            dplyr.summarise(
                pagado_sgf = base.sum_(f.importe_bruto),
                _groups = 'drop')
        control_mes = slave_mes >> \
            dplyr.full_join(sgf_mes) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            ) >> \
            dplyr.mutate(
                diferencia = f.ejecutado_slave - f.pagado_sgf
            )
        #     dplyr.filter_(~dplyr.near(f.diferencia, 0))
        control_mes.sort_values(by=['mes', 'cta_cte'], inplace= True)
        control_mes = pd.DataFrame(control_mes)
        control_mes['dif_acum'] = control_mes['diferencia'].cumsum()
        control_mes.reset_index(drop=True, inplace=True)
        return control_mes

    # --------------------------------------------------
    def import_slave(self):
        df = super().import_slave(ejercicio = self.ejercicio)
        cta_cte = self.import_siif_rcg01_uejp(self.ejercicio)
        cta_cte = cta_cte.loc[:, ['nro_comprobante', 'cta_cte']]
        df = df.merge(cta_cte, on='nro_comprobante', how='left')
        self.slave = df
        return self.slave

    # --------------------------------------------------
    def control_mes_beneficiario(self):
        slave_mes = self.slave.copy()
        slave_mes = slave_mes >> \
            dplyr.select(f.mes, f.cta_cte, f.razon_social, f.importe_bruto) >> \
            dplyr.rename(beneficiario = f.razon_social) >> \
            dplyr.group_by(f.mes, f.cta_cte, f.beneficiario) >> \
            dplyr.summarise(ejecutado_slave = base.sum_(f.importe_bruto),
                            _groups = 'drop')
        sgf_mes = self.sgf_resumen_rend_honorarios.copy()
        sgf_mes = sgf_mes >> \
            dplyr.select(
                f.mes, f.cta_cte, f.beneficiario, f.importe_bruto
            ) >> \
            dplyr.group_by(f.mes, f.cta_cte, f.beneficiario) >> \
            dplyr.summarise(
                pagado_sgf = base.sum_(f.importe_bruto),
                _groups = 'drop')
        control_mes = slave_mes >> \
            dplyr.full_join(sgf_mes) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            ) >> \
            dplyr.mutate(
                diferencia = f.ejecutado_slave - f.pagado_sgf
            )
        #     dplyr.filter_(~dplyr.near(f.diferencia, 0))
        control_mes.sort_values(by=['mes', 'cta_cte', 'beneficiario'], inplace= True)
        control_mes = pd.DataFrame(control_mes)
        control_mes['dif_acum'] = control_mes['diferencia'].cumsum()
        control_mes.reset_index(drop=True, inplace=True)
        return control_mes

    # # --------------------------------------------------
    # def control_completo(self):
    #     siif = self.siif_comprobantes_haberes_neto_rdeu.copy()
    #     siif = siif >> \
    #         dplyr.rename_with(lambda x: 'siif_' + x) >> \
    #         dplyr.rename(
    #             ejercicio = f.siif_ejercicio,
    #             mes = f.siif_mes
    #         )
    #     sscc = self.sscc_banco_invico.copy()
    #     sscc = sscc >> \
    #         dplyr.rename_with(lambda x: 'sscc_' + x) >> \
    #         dplyr.rename(
    #             ejercicio = f.sscc_ejercicio,
    #             mes = f.sscc_mes,
    #         )
    #     control_completo = sscc >> \
    #         dplyr.full_join(siif) >> \
    #         dplyr.mutate(
    #             dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
    #         ) >> \
    #         dplyr.mutate(
    #             diferencia = f.siif_importe - f.sscc_importe
    #         )
    #     control_completo.sort_values(
    #         by=['mes'], 
    #         inplace= True)
    #     control_completo = pd.DataFrame(control_completo)
    #     control_completo.reset_index(drop=True, inplace=True)
    #     return control_completo