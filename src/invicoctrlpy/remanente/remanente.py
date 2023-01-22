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
    - Saldos por cuenta Banco INVICO (SSCC) al 31/12 de cada año (¿de dónde saco esto?)
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

from invicoctrlpy.utils.import_dataframe import ImportDataFrame


@dataclass
# --------------------------------------------------
class Remamente(ImportDataFrame):
    ejercicio:str = str(dt.datetime.now().year)
    db_path:str = None
    update_db:bool = False
    siif_desc_pres = pd.DataFrame = field(init=False, repr=False)

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

        # update_icaro = update_db.UpdateIcaro(
        #     self.get_outside_path() + '/R Output/SQLite Files/ICARO.sqlite', 
        #     self.db_path + '/icaro.sqlite')
        # update_icaro.migrate_icaro()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.siif_desc_pres = self.import_siif_desc_pres(ejercicio_to=self.ejercicio)

