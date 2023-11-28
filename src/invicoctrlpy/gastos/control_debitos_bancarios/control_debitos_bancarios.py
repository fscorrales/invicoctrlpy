#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control de Debitos Bancarios
Data required:
    - SIIF rcg01_uejp
    - SIIF gto_rpa03g
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
 - invicoctrlpy (pip install -e '/home/kanou/IT/R Apps/R Gestion INVICO/invicoctrlpy')
"""

import datetime as dt
import os
from dataclasses import dataclass, field
from typing import List

import pandas as pd
import numpy as np
from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicodb.update import update_db


def default_ejercicio():
    return [str(dt.datetime.now().year)]

@dataclass
# --------------------------------------------------
class ControlDebitosBancarios(ImportDataFrame):
    """
    Represents a controller for managing and processing bank debits data.

    Attributes:
        ejercicio (List[str]): List of strings representing the fiscal year.
            Default value is obtained from a factory function (default_ejercicio).
        input_path (str): Path to the input directory for data import. Can be left as None if not provided.
        db_path (str): Path to the directory or location of the database. Can be left as None if not provided.
        update_db (bool): Indicates if a database update should be performed.
            Default is set to False.

    This class is expected to be inherited from a base class 'ImportDataFrame'
    and provides functionalities related to importing, processing, and controlling
    specific bank debit-related data. Its methods and additional attributes
    are expected to complement and extend the basic functionalities provided by
    the base class.
    """
    ejercicio: List[str] = field(default_factory=default_ejercicio)
    input_path:str = None
    db_path:str = None
    update_db:bool = False

    # --------------------------------------------------
    def __post_init__(self):
        """
        Initializes the ControlDebitosBancarios class.

        Performs specific actions after the initialization of an object of
        the class, such as obtaining the database path, updating the database
        (if necessary), and importing relevant DataFrames.

        Args:
            None
        Returns:
            None
        """
        if self.db_path == None:
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
        if self.input_path == None:
            update_path_input = self.get_update_path_input()
        else:
            update_path_input = self.input_path

        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_banco_invico()

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_comprobantes_gtos_rcg01_uejp()
        update_siif.update_comprobantes_gtos_gpo_part_gto_rpa03g()

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
    def import_siif_debitos(self) -> pd.DataFrame:
        """
        Imports data related to debits from the SIIF (Sistema Integrado de Información Financiera).

        Returns:
            pd.DataFrame: DataFrame containing the imported debits data from SIIF.

        This method uses the 'import_siif_comprobantes' method from the base class to retrieve
        data related to comprobantes (vouchers) from the SIIF system for the specified fiscal year.
        The data is filtered to only include entries with a specific 'partida' value (e.g., '355').
        The imported data is structured into a DataFrame and returned for further processing.

        Example:
            To import data for 'debitos bancarios' from 'siif' for 
            the specified fiscal year:

            ```python
            debitos = control.import_siif_debitos()
            ```
        """
        df = super().import_siif_comprobantes(ejercicio = self.ejercicio)
        df = df.loc[df['partida'] == '355']
        # keep = ['HONOR', 'RECON', 'LOC']
        # df = df.loc[df.glosa.str.contains('|'.join(keep))]
        df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def siif_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:
        """
        Summarize SIIF data for the specified groupby columns.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by. 
                Defaults to ['ejercicio', 'mes', 'cta_cte']. It could also include 'fecha'

        Returns:
            pd.DataFrame: A DataFrame summarizing the SIIF data based on the specified groupby columns.

        This method imports and processes SIIF data related to the 'Debitos Bancarios' category. It then groups
        the data by the specified columns, calculates the sum of numeric values, and returns the resulting
        DataFrame. Any missing values are filled with zeros.

        Example:
            To summarize 'siif' data based on custom grouping columns:

            ```python
            siif_summary = control.siif_summarize(groupby_cols=['ejercicio', 'mes'])
            ```
        """
        siif = self.import_siif_debitos().copy()
        siif = siif.groupby(groupby_cols).sum(numeric_only=True)
        siif = siif.reset_index()
        df = siif
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    def import_banco_invico(self) -> pd.DataFrame:
        """
        Imports data related to debits from the SIIF (Sistema Integrado de Información Financiera).

        Returns:
            pd.DataFrame: DataFrame containing the imported debits data from SIIF.

        This method uses the 'import_siif_comprobantes' method from the base class to retrieve
        data related to comprobantes (vouchers) from the SIIF system for the specified fiscal year.
        The data is filtered to only include entries with a specific 'partida' value (e.g., '355').
        The imported data is structured into a DataFrame and returned for further processing.
        """
        df = super().import_banco_invico(ejercicio = self.ejercicio)
        df = df.loc[df['cod_imputacion'] == '031']
        return df

    # --------------------------------------------------
    def sscc_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:
        """
        Summarize and filter bank data related to INVICO (Instituto de Vivienda de Corrientes).

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for
                summarization. Defaults to ['ejercicio', 'mes', 'cta_cte']. 
                It could also include 'fecha'

        Returns:
            pd.DataFrame: A DataFrame containing summarized and filtered bank data related to INVICO.

        This method imports bank data related to INVICO for the specified exercise (year),
        filters out specific 'cod_imputacion' values, and summarizes the data based on the
        provided groupby columns. The resulting DataFrame contains the summarized and
        filtered bank data for further analysis.
        """
        df = self.import_banco_invico().copy()
        df = df.drop(['es_cheque'], axis=1)
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        df['importe'] = df['importe'] * -1
        return df

    # --------------------------------------------------
    def siif_vs_sscc(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte'],
        only_diff = False
    ) -> pd.DataFrame:
        """
        Perform cross-control analysis between SIIF (Sistema Integrado de Información 
        Financiera) data and SSCC (Sistema de Seguimiento de Cuentas Corrientes) data.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for
                comparison. Defaults to ['ejercicio', 'mes', 'cta_cte'].
            only_diff (bool, optional): If True, returns only the differing records.
                Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing the comparison results between SIIF and SSCC.

        This method compares and reconciles the summarized data from SIIF and SSCC based on
        the provided groupby columns. It aligns the data and checks for differences or
        discrepancies between the two datasets. If `only_diff` is set to True, it returns
        only the records where differences exist. Otherwise, it returns the full comparison.

        Example:
            To perform a cross-control analysis based on custom grouping columns:

            ```python
            result = control.siif_vs_sscc(groupby_cols=['ejercicio', 'mes'])
            ```
        """
        siif = self.siif_summarize(groupby_cols=groupby_cols).copy()
        siif = siif.rename(columns={'importe': 'ejecutado_siif'})
        siif = siif.set_index(groupby_cols)
        sscc = self.sscc_summarize(groupby_cols=groupby_cols).copy()
        sscc = sscc.rename(columns={'importe': 'debitos_sscc'})
        sscc = sscc.set_index(groupby_cols)
        # Obtener los índices faltantes en siif
        missing_indices = sscc.index.difference(siif.index)
        # Reindexar el DataFrame siif con los índices faltantes
        siif = siif.reindex(siif.index.union(missing_indices))
        sscc = sscc.reindex(siif.index)
        siif = siif.fillna(0)
        sscc = sscc.fillna(0)
        df = siif.merge(sscc, how='outer', on=groupby_cols)
        df = df.reset_index()
        df = df.fillna(0)
        #Reindexamos el DataFrame
        # siif = siif.reset_index()
        # df = df.reindex(columns=siif.columns)
        if only_diff:
            # Seleccionar solo las columnas numéricas
            numeric_cols = df.select_dtypes(include=np.number).columns
            # Filtrar el DataFrame utilizando las columnas numéricas válidas
            df = df[df[numeric_cols].sum(axis=1) != 0]
            df = df.reset_index(drop=True)
        return df