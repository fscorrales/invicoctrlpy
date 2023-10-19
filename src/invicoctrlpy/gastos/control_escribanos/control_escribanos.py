#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control de Escribanos (FEI)
Data required:
    - SGF Resumen Rend por Proveedor
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
    - SIIF rcocc31 (
        2113-2-9 Escribanos)
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
class ControlEscribanos(ImportDataFrame):
    """
    Class for financial retention control.

    This class inherits from ImportDataFrame and provides specific functionality
    for importing and controlling financial data related to escribanos (FEI).

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
        To create an instance of ControlEscribanos with a custom list of fiscal years:

        ```python
        control = ControlEscribanos(ejercicio=['2023', '2022', '2021'])
        ```
    """
    ejercicio: List[str] = field(default_factory=default_ejercicio)
    input_path:str = None
    db_path:str = None
    update_db:bool = False

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
        update_siif.update_mayor_contable_rcocc31()

        update_sgf = update_db.UpdateSGF(
            update_path_input + '/Sistema Gestion Financiera', 
            self.db_path + '/sgf.sqlite')
        update_sgf.update_resumen_rend_prov()
        update_sgf.update_listado_prov()

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
    def import_siif_escribanos(self) -> pd.DataFrame:
        """
        Import and process SIIF data related to the "Escribanos" category.

        Returns:
            pd.DataFrame: A DataFrame containing SIIF data for Escribanos.

        This method retrieves SIIF data for the specified exercise and account code '2113-2-9' 
        (which corresponds to Escribanos). It filters out entries with the 'tipo_comprobante' 
        not equal to 'APE' (AJU, ANP, APE, CIE, FEI, PFE) and returns the resulting DataFrame.

        Example:
            To import data for 'escribanos' from 'siif' for 
            the specified fiscal year:

            ```python
            escribanos = control.import_siif_escribanos()
            ```
        """
        siif = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '2113-2-9'
        )
        siif = siif.loc[siif['tipo_comprobante'] != 'APE'] #AJU, ANP, APE, CIE, FEI, PFE
        siif = siif.rename(columns={
            'auxiliar_1': 'cuit',
            'creditos': 'carga_fei',
            'debitos':'pagos_fei'
        })
        siif['dif_carga_pagos'] = siif['carga_fei'] - siif['pagos_fei']
        df = siif
        return df

    # --------------------------------------------------
    def siif_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cuit']
    ) -> pd.DataFrame:
        """
        Summarize SIIF data for the specified groupby columns.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by. 
                Defaults to ['ejercicio', 'mes', 'cuit'].

        Returns:
            pd.DataFrame: A DataFrame summarizing the SIIF data based on the specified groupby columns.

        This method imports and processes SIIF data related to the 'Escribanos' category. It then groups
        the data by the specified columns, calculates the sum of numeric values, and returns the resulting
        DataFrame. Any missing values are filled with zeros.

        Example:
            To summarize 'siif' data based on custom grouping columns:

            ```python
            siif_summary = control.siif_summarize(groupby_cols=['ejercicio', 'mes'])
            ```
        """
        siif = self.import_siif_escribanos().copy()
        siif = siif.drop([
            'tipo_comprobante', 'fecha', 'fecha_aprobado', 'cta_contable',
            'auxiliar_2', 'saldo', 'nro_entrada'
        ], axis=1)
        siif = siif.groupby(groupby_cols).sum(numeric_only=True)
        siif = siif.reset_index()
        df = siif
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    def import_resumen_rend_cuit(self) -> pd.DataFrame:
        """
        Import and filter a summary of renditions by CUIT (Tax ID).

        Args:
            None

        Returns:
            pd.DataFrame: Pandas DataFrame containing summary data for rend cuit.

        This method imports a summary of renditions and filters it based on the CUIT (Tax ID)
        '130832-08'. The filtered DataFrame contains information about renditions, and it is returned
        for further processing or analysis.
        """
        df = super().import_resumen_rend_cuit(
            ejercicio = self.ejercicio, neto_cert_neg=False
        )
        df = df.loc[df['cta_cte'] == '130832-08']
        return df

    # --------------------------------------------------
    def sgf_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cuit', 'beneficiario']
    ) -> pd.DataFrame:
        """
        Summarize renditions data related to the SGF

        Args:
            groupby_cols (list, optional): A list of columns to group by in the summary. Defaults to
            ['ejercicio', 'mes', 'cuit', 'beneficiario'].

        Returns:
            pd.DataFrame: Pandas DataFrame containing the cross-controlled data.

        This method imports renditions data, removes irrelevant columns, and then summarizes the
        data based on the specified grouping columns. The resulting DataFrame contains the summary
        of renditions data related to the SGF for further analysis.
        """
        df = self.import_resumen_rend_cuit().copy()
        df = df.drop(
            ['origen', 'fecha','destino', 'libramiento_sgf',
            'seguro', 'salud', 'mutual', 'otras', 'importe_bruto',
            'gcias', 'iibb', 'sellos', 'suss', 'invico', 'retenciones'], 
            axis=1
        )
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        return df

    # --------------------------------------------------
    def import_banco_invico(self) -> pd.DataFrame:
        """
        Import and filter bank data related to INVICO (Instituto de Vivienda de Corrientes).

        Returns:
            pd.DataFrame: A DataFrame containing bank data related to INVICO, filtered for the
            specific account '130832-08'.

        This method imports bank data related to INVICO for the specified exercise (year) and filters
        it to include only records associated with the account '130832-08'. The resulting DataFrame
        contains the filtered bank data for further analysis.
        """
        df = super().import_banco_invico(ejercicio = self.ejercicio)
        df = df.loc[df['cta_cte'] == '130832-08']
        return df

    # --------------------------------------------------
    def sscc_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cod_imputacion', 'imputacion']
    ) -> pd.DataFrame:
        """
        Summarize and filter bank data related to INVICO (Instituto de Vivienda de Corrientes).

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for
                summarization. Defaults to ['ejercicio', 'mes', 'cod_imputacion', 'imputacion'].

        Returns:
            pd.DataFrame: A DataFrame containing summarized and filtered bank data related to INVICO.

        This method imports bank data related to INVICO for the specified exercise (year),
        filters out specific 'cod_imputacion' values, and summarizes the data based on the
        provided groupby columns. The resulting DataFrame contains the summarized and
        filtered bank data for further analysis.
        """
        df = self.import_banco_invico().copy()
        filtrar = [
            '004', '034', '213', '102' 
        ]
        df = df.loc[
            ~df['cod_imputacion'].isin(filtrar)
        ]
        df = df.drop(['es_cheque'], axis=1)
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        df['importe'] = df['importe'] * -1
        df = df.rename(columns={'importe':'importe_neto'})
        return df

    # --------------------------------------------------
    def sgf_vs_sscc(
        self, groupby_cols:List[str] = ['ejercicio', 'mes'],
        only_diff = False
    ) -> pd.DataFrame:
        """
        Perform a comparison between 'sgf' and 'sscc' data.

        This method compares summarized financial data from the SGF system with summarized
        accounting and control data from the SSCC system. It calculates the differences
        between the two datasets based on the provided groupby columns. The resulting
        DataFrame contains the comparison results, and if 'only_diff' is True, it includes
        only rows with non-zero differences.

        Args:
            groupby_cols (List[str], optional): A list of column names to group and
                perform the comparison on. Default is ['ejercicio', 'mes', 'cta_cte'].
            only_diff (bool, optional): Flag indicating whether to return only rows with
                differences. Default is False.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the comparison results, including
            the differences between 'sgf' and 'sscc' data.

        Example:
            To compare 'sgf' and 'sscc' data based on custom grouping columns:

            ```python
            result = control.sgf_vs_sscc(groupby_cols=['ejercicio', 'mes'])
            ```
        """
        sgf =  self.sgf_summarize(groupby_cols=groupby_cols).copy()
        sgf = sgf.set_index(groupby_cols)
        sscc = self.sscc_summarize(groupby_cols=groupby_cols).copy()
        sscc = sscc.set_index(groupby_cols)
        # Obtener los índices faltantes en sgf
        missing_indices = sscc.index.difference(sgf.index)
        # Reindexar el DataFrame sgf con los índices faltantes
        sgf = sgf.reindex(sgf.index.union(missing_indices))
        sscc = sscc.reindex(sgf.index)
        sgf = sgf.fillna(0)
        sscc = sscc.fillna(0)
        df = sgf.subtract(sscc)
        df = df.reset_index()
        df = df.fillna(0)
        #Reindexamos el DataFrame
        sgf = sgf.reset_index()
        df = df.reindex(columns=sgf.columns)
        if only_diff:
            # Seleccionar solo las columnas numéricas
            numeric_cols = df.select_dtypes(include=np.number).columns
            # Filtrar el DataFrame utilizando las columnas numéricas válidas
            df = df[df[numeric_cols].sum(axis=1) != 0]
            df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def siif_vs_sgf(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cuit'],
        only_diff = False
    ) -> pd.DataFrame:
        """
        Perform cross-control analysis between 'siif' and 'sgf' data.

        This method compares summarized financial data from the SIIF system with summarized
        financial data from the SGF system. It calculates the differences between the two
        datasets based on the provided groupby columns. The resulting DataFrame contains the
        comparison results, and if 'only_diff' is True, it includes only rows with non-zero
        differences.

        Args:
            groupby_cols (List[str], optional): A list of column names to group and
                perform cross-control analysis on. Default is ['ejercicio', 'mes', 'cta_cte'].
            only_diff (bool, optional): Flag indicating whether to return only rows with
                differences. Default is False.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the results of the cross-control
            analysis.

        Example:
            To perform a cross-control analysis based on custom grouping columns:

            ```python
            result = control.siif_vs_sgf(groupby_cols=['ejercicio', 'mes'])
            ```
        """
        siif = self.siif_summarize(groupby_cols=groupby_cols).copy()
        siif = siif.set_index(groupby_cols)
        sgf = self.sgf_summarize(groupby_cols=groupby_cols).copy()
        sgf = sgf.set_index(groupby_cols)
        # Obtener los índices faltantes en siif
        missing_indices = sgf.index.difference(siif.index)
        # Reindexar el DataFrame siif con los índices faltantes
        siif = siif.reindex(siif.index.union(missing_indices))
        sgf = sgf.reindex(siif.index)
        siif = siif.fillna(0)
        sgf = sgf.fillna(0)
        df = siif.subtract(sgf)
        df = df.reset_index()
        df = df.fillna(0)
        #Reindexamos el DataFrame
        siif = siif.reset_index()
        df = df.reindex(columns=siif.columns)
        if only_diff:
            # Seleccionar solo las columnas numéricas
            numeric_cols = df.select_dtypes(include=np.number).columns
            # Filtrar el DataFrame utilizando las columnas numéricas válidas
            df = df[df[numeric_cols].sum(axis=1) != 0]
            df = df.reset_index(drop=True)
        return df