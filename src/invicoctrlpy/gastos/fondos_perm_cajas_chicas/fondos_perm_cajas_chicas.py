#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: SIIF Fondos Permanentes y Cajas Chicas
Data required:
    - SIIF rcg01_uejp
    - SIIF gto_rpa03g
    - SIIF rog01
    - SSCC ctas_ctes (manual data)
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


@dataclass
# --------------------------------------------------
class FondosPermCajasChicas(ImportDataFrame):
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
        update_siif.update_detalle_partidas_rog01()
        
        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()

    # --------------------------------------------------
    def import_siif_comprobantes_fondos_perm(self) -> pd.DataFrame:
        return super().import_siif_comprobantes_fondos_perm(ejercicio = self.ejercicio)

    # # --------------------------------------------------
    # def control_mes_grupo(self):
    #     siif_mes_gpo = self.import_siif_rci02()
    #     siif_mes_gpo = siif_mes_gpo >> \
    #         dplyr.select(f.mes, f.grupo, f.importe) >> \
    #         dplyr.group_by(f.mes, f.grupo) >> \
    #         dplyr.summarise(recursos_siif = base.sum_(f.importe),
    #                         _groups = 'drop')
    #     sscc_mes_gpo = self.import_banco_invico()
    #     sscc_mes_gpo = sscc_mes_gpo >> \
    #         dplyr.select(
    #             f.mes, f.grupo, 
    #             f.importe
    #         ) >> \
    #         dplyr.group_by(f.mes, f.grupo) >> \
    #         dplyr.summarise(
    #             depositos_sscc = base.sum_(f.importe),
    #             _groups = 'drop')
    #     control_mes_gpo = siif_mes_gpo >> \
    #         dplyr.full_join(sscc_mes_gpo) >> \
    #         dplyr.mutate(
    #             dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
    #         ) >> \
    #         dplyr.mutate(
    #             diferencia = f.recursos_siif - f.depositos_sscc
    #         )
    #         # dplyr.filter_(~dplyr.near(f.diferencia, 0))
    #     control_mes_gpo.sort_values(by=['mes', 'grupo'], inplace= True)
    #     control_mes_gpo = pd.DataFrame(control_mes_gpo)
    #     control_mes_gpo.reset_index(drop=True, inplace=True)
    #     return control_mes_gpo
