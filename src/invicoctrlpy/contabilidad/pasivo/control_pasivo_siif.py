#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control de RDEU SIIF para análisis de Pasivo
Data required:
    - SIIF rdeu
    - SIIF rcocc31 (1112-2-6 Banco)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
from dataclasses import dataclass, field
from typing import List

import pandas as pd
from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicoctrlpy.recursos.control_recursos.control_recursos import ControlRecursos
from invicoctrlpy.gastos.control_obras.control_obras import ControlObras
from invicoctrlpy.gastos.control_haberes.control_haberes import ControlHaberes
from invicoctrlpy.gastos.control_honorarios.control_honorarios import ControlHonorarios
from invicoctrlpy.gastos.control_debitos_bancarios.control_debitos_bancarios import ControlDebitosBancarios
from invicoctrlpy.gastos.control_escribanos.control_escribanos import ControlEscribanos
from invicodb.update import update_db


def default_ejercicio():
    return [str(dt.datetime.now().year)]

@dataclass
# --------------------------------------------------
class ControlPasivo(ImportDataFrame):
    """
    Class for financial retention control.

    This class inherits from ImportDataFrame and provides specific functionality
    for importing and controlling financial data related to retentions.

    Args:
        ejercicio (List[str]): A list of fiscal years as strings. Default is the
            current year as a single-element list.
        input_path (str): Optional input path.
        db_path (str): Optional database path.
        update_db (bool): Boolean indicator to perform updates in the database.

    Attributes:
        ejercicio (List[str]): A list of fiscal years as strings.
        input_path (str): Optional input path.
        db_path (str): Optional database path.
        update_db (bool): Boolean indicator to perform updates
            in the database.
        icaro_carga (pd.DataFrame): Pandas DataFrame to store "Icaro Carga" DB.

    Example:
        To create an instance of ControlRetenciones with a custom list of fiscal years:

        ```python
        control = ControlRetenciones(ejercicio=['2023', '2022', '2021'])
        ```
    """
    ejercicio: List[str] = field(default_factory=default_ejercicio)
    input_path:str = None
    db_path:str = None
    update_db:bool = False
    icaro_carga:pd.DataFrame = field(init=False, repr=False)
    control_recursos:ControlRecursos = field(init=False, repr=False)
    control_obras:ControlObras = field(init=False, repr=False)
    control_haberes:ControlHaberes = field(init=False, repr=False)
    control_honorarios:ControlHonorarios = field(init=False, repr=False)
    control_debitos_bancarios:ControlDebitosBancarios = field(init=False, repr=False)
    control_escribanos:ControlEscribanos = field(init=False, repr=False)

    # --------------------------------------------------
    def __post_init__(self):
        """
        Initializes the ControlRetenciones class.

        Performs specific actions after the initialization of an object of
        the class, such as obtaining the database path, updating the database
        (if necessary), and importing relevant DataFrames.

        Args:
            None
        Returns:
            None
        """
        if self.db_path is None:
            self.get_db_path()
        if self.update_db:
            self.update_sql_db()
        self.import_dfs()

    # --------------------------------------------------
    def update_sql_db(self):
        """
        Performs updates in the database.

        This method carries out several updates in the database, including
        migrations and updates of different datasets.

        Args:
            None
        Returns:
            None
        """
        if self.input_path is None:
            update_path_input = self.get_update_path_input()
        else:
            update_path_input = self.input_path

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_mayor_contable_rcocc31()
        update_siif.update_deuda_flotante_rdeu012()

    # --------------------------------------------------
    def import_dfs(self):
        """
        Imports relevant DataFrames.

        Calls specific import methods to import necessary datasets.

        Args:
            None
        Returns:
            None
        """
        self.import_ctas_ctes()

    # --------------------------------------------------
    def pa6_siif(self) -> pd.DataFrame:
        df = self.import_siif_rfondo07tp_pa6(self.ejercicio)
        return df

    # --------------------------------------------------
    def control_recursos_siif(self) -> pd.DataFrame:
        df = self.control_recursos.control_mes_grupo()
        return df

    # --------------------------------------------------
    def control_obras_siif(self) -> pd.DataFrame:
        df = self.control_obras.control_cruzado()
        return df

    # --------------------------------------------------
    def control_haberes_siif(self) -> pd.DataFrame:
        df = self.control_haberes.control_cruzado()
        return df

    # --------------------------------------------------
    def control_honorarios_siif(self) -> pd.DataFrame:
        df = self.control_honorarios.slave_vs_sgf(only_diff=True)
        return df

    # --------------------------------------------------
    def control_debitos_bancarios_siif(self) -> pd.DataFrame:
        df = self.control_debitos_bancarios.siif_vs_sscc()
        return df

    # --------------------------------------------------
    def control_escribanos_siif(self) -> pd.DataFrame:
        df = self.control_escribanos.siif_vs_sgf()
        return df

    # --------------------------------------------------
    def banco_invico_saldo_final(self) -> pd.DataFrame:
        df = self.import_sdo_final_banco_invico(ejercicio = self.ejercicio)
        return df
