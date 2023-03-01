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
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_recuperos.update_saldo_barrio()
        update_recuperos.update_saldo_barrio_variacion()
        update_recuperos.update_saldo_recuperos_cobrar_variacion()
        update_recuperos.update_saldo_motivo()
        update_recuperos.update_saldo_motivo_entrega_viviendas()
        update_recuperos.update_barrios_nuevos()
        update_recuperos.update_resumen_facturado()
        update_recuperos.update_resumen_recaudado()

    def control_suma_saldo_barrio_variacion(self):
        df = self.import_saldo_barrio_variacion(self.ejercicio)
        df = df.groupby(["ejercicio"])[["saldo_inicial", "amortizacion", "cambios", "saldo_final"]].sum()
        df["suma_algebraica"] = df.saldo_inicial + df.amortizacion + df.cambios
        df["dif_saldo_final"] = df.saldo_final - df.suma_algebraica
        return df

    def control_saldos_recuperos_cobrar_variacion(self):
        df = self.import_saldo_recuperos_cobrar_variacion(self.ejercicio)
        df = df.loc[df['concepto'].isin(['SALDO AL INICIO:', 'SALDO AL FINAL:'])]
        return df