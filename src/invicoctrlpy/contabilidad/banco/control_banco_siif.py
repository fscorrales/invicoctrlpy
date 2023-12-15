#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control de Banco SIIF vs Real INVICO (SSCC)
Data required:
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
    - SIIF rcocc31 (1112-2-6 Banco)
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
class ControlBanco(ImportDataFrame):
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

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_mayor_contable_rcocc31()

        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_banco_invico()

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
    def import_banco_siif(self) -> pd.DataFrame:
 
        siif_banco = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '1112-2-6'
        )
        siif_banco = siif_banco.rename(columns={
            'auxiliar_1': 'cta_cte'
        })
        map_to = self.ctas_ctes.loc[:,['map_to', 'siif_contabilidad_cta_cte']]
        df = siif_banco
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='siif_contabilidad_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'siif_contabilidad_cta_cte'], axis='columns', inplace=True)
        return df

    # --------------------------------------------------
    def banco_siif_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:

        banco_siif = self.import_banco_siif().copy()
        banco_siif = banco_siif.groupby(groupby_cols).sum(numeric_only=True)
        banco_siif = banco_siif.reset_index()
        df = banco_siif
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    def icaro_vs_siif(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte'],
        only_diff = False
    ) -> pd.DataFrame:
        """
        Compare 'icaro' data with 'siif' data.

        This method performs a comparison between summarized 'icaro' data and summarized
        'siif' (Sistema de Información Integrado de Finanzas) data based on the specified
        grouping columns. It calculates the differences between the two datasets and can
        optionally filter and return only rows with differences.

        Args:
            groupby_cols (List[str], optional): A list of column names to group and
                perform the comparison on. Default is ['ejercicio', 'mes', 'cta_cte'].
            only_diff (bool, optional): Flag indicating whether to return only rows with
                differences. Default is False.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the comparison results between
            'icaro' and 'siif' data.

        Example:
            To compare 'icaro' and 'siif' data based on custom grouping columns and
            return only rows with differences:

            ```python
            diff_data = control.icaro_vs_siif(groupby_cols=['ejercicio', 'mes'], only_diff=True)
            ```
        """
        icaro = self.icaro_summarize(groupby_cols=groupby_cols).copy()
        icaro = icaro.set_index(groupby_cols)
        siif = self.banco_siif_summarize(groupby_cols=groupby_cols).copy()
        siif = siif.set_index(groupby_cols)
        # Obtener los índices faltantes en icaro
        missing_indices = siif.index.difference(siif.index)
        # Reindexar el DataFrame icaro con los índices faltantes
        icaro = icaro.reindex(icaro.index.union(missing_indices))
        siif = siif.reindex(icaro.index)
        icaro = icaro.fillna(0)
        siif = siif.fillna(0)
        df = icaro.subtract(siif)
        df = df.reset_index()
        df = df.fillna(0)
        #Reindexamos el DataFrame
        icaro = icaro.reset_index()
        df = df.reindex(columns=icaro.columns)
        if only_diff:
            # Seleccionar solo las columnas numéricas
            numeric_cols = df.select_dtypes(include=np.number).columns
            # Filtrar el DataFrame utilizando las columnas numéricas válidas
            df = df[df[numeric_cols].sum(axis=1) != 0]
            df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def import_resumen_rend_cuit(self) -> pd.DataFrame:
        """
        Imports SGF's summary data related to with cuit for the specified fiscal year.

        Args:
            None

        Returns:
            pd.DataFrame: Pandas DataFrame containing summary data for rend cuit.
        """
        return super().import_resumen_rend_cuit(
            ejercicio = self.ejercicio, neto_cert_neg=False)

    # --------------------------------------------------
    def import_banco_invico(self) -> pd.DataFrame:
        """
        Imports Banco Invico data for he specified fiscal year.

        Args:
            None

        Returns:
            pd.DataFrame: Pandas DataFrame containing Banco Invico data.
        """
        return super().import_banco_invico()

    # --------------------------------------------------
    def sscc_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:
        """
        Perform cross-control of Banco INVICO's data.

        Performs cross-control of the imported data by grouping it based on
        the specified columns and summing the values within each group.

        Args:
            groupby_cols (list): List of column names to group by.

        Returns:
            pd.DataFrame: Pandas DataFrame containing the cross-controlled data.
        """
        df = self.import_banco_invico().copy()
        inversion_obras = [
            '018', '019', '020', '021', '027', '035', '041',
            '052', '053', '065', '066', '072', '112', '142'
            '143', '162', '210', '213', '217', '219', '221',
            '225', '227','034'
        ]
        df = df.loc[
            df['cod_imputacion'].isin(inversion_obras)
        ]
        df['retenciones'] = df.loc[df['cod_imputacion'] == '034']['importe'] * -1
        # Filtrar los registros que cumplan ambas condiciones
        # ['iibb', 'sellos', 'lp', 'gcias', 'suss', 'invico']
        df.loc[(df['concepto'].str.contains('IIBB')) & (df['cod_imputacion'] == '034'), 'iibb'] = df['retenciones']
        df.loc[(df['concepto'].str.contains('SELLOS')) & (df['cod_imputacion'] == '034'), 'sellos'] = df['retenciones']
        df.loc[(df['concepto'].str.contains('GCI')) & (df['cod_imputacion'] == '034'), 'gcias'] = df['retenciones']
        df.loc[(df['concepto'].str.contains('SUSS')) & (df['cod_imputacion'] == '034'), 'suss'] = df['retenciones']
        df.loc[(df['concepto'].str.contains('INV')) & (df['cod_imputacion'] == '034'), 'invico'] = df['retenciones']
        #df['otra_ret'] = df['retenciones'] - df['iibb'] - df['sellos'] - df['gcias'] - df['suss'] - df['invico']
        df['importe'] = df.loc[df['cod_imputacion'] != '034']['importe']
        df = df.drop(
            ['fecha', 'es_cheque','beneficiario', 'concepto', 'moneda',
            'libramiento', 'cod_imputacion', 'imputacion'], 
            axis=1
        )
        df = df.fillna(0)
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        df['importe'] = df['importe'] * -1
        df = df.rename(columns={'importe':'importe_neto'})
        df['importe_bruto'] = df['importe_neto'] + df['retenciones']
        return df

    # --------------------------------------------------
    def sgf_vs_sscc(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte'],
        only_diff = False
    ) -> pd.DataFrame:
        """
        Perform a comparison between 'sgf' and 'sscc' data.

        This method compares summarized 'sgf' (Sistema Gestion Financiera) data with
        summarized 'sscc' (Sistema de Seguimiento de Cuentas Corrientes) data based on
        the specified grouping columns. It calculates the differences in the 'importe'
        columns between 'sgf' and 'sscc'.

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
        missing_indices = sscc.index.difference(sscc.index)
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
    def icaro_vs_sgf(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte'],
        only_diff = False
    ) -> pd.DataFrame:
        """
        Perform cross-control analysis between 'icaro' and 'invico' data.

        This method calculates the cross-control analysis between summarized 'icaro' data
        and summarized 'invico' data based on the specified grouping columns. It subtracts
        the values of 'invico' from 'icaro' to identify discrepancies.

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
            result = control.icaro_vs_invico(groupby_cols=['ejercicio', 'mes'])
            ```
        """
        icaro = self.icaro_summarize(groupby_cols=groupby_cols).copy()
        icaro['sellos'] = icaro['sellos'] + icaro['lp']
        icaro = icaro.drop(columns=['lp'])
        icaro = icaro.set_index(groupby_cols)
        invico = self.sgf_summarize(groupby_cols=groupby_cols).copy()
        invico = invico.set_index(groupby_cols)
        # Obtener los índices faltantes en icaro
        missing_indices = invico.index.difference(icaro.index)
        # Reindexar el DataFrame icaro con los índices faltantes
        icaro = icaro.reindex(icaro.index.union(missing_indices))
        invico = invico.reindex(icaro.index)
        icaro = icaro.fillna(0)
        invico = invico.fillna(0)
        df = icaro.subtract(invico)
        df = df.reset_index()
        df = df.fillna(0)
        #Reindexamos el DataFrame
        icaro = icaro.reset_index()
        df = df.reindex(columns=icaro.columns)
        if only_diff:
            # Seleccionar solo las columnas numéricas
            numeric_cols = df.select_dtypes(include=np.number).columns
            # Filtrar el DataFrame utilizando las columnas numéricas válidas
            df = df[df[numeric_cols].sum(axis=1) != 0]
            df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def icaro_vs_sscc(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte'],
        only_diff = False
    ) -> pd.DataFrame:
        """
        Compares data between the 'icaro' and 'sscc' summaries.

        This method calculates the difference between the 'icaro' and 'sscc' summaries
        based on the specified grouping columns. It optionally allows you to filter
        and return only rows with differences.

        Args:
            groupby_cols (list, optional): A list of column names to group by.
                Defaults to ['ejercicio', 'mes', 'cta_cte'].
            only_diff (bool, optional): If True, returns only rows with differences.
                Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing the comparison results.

        Notes:
            - The 'sellos' and 'lp' columns in 'icaro' are combined into a single 'sellos' column.
            - The resulting DataFrame is reindexed to match the 'icaro' DataFrame columns.
        
        Example:
            To perform a cross-control analysis based on custom grouping columns:

            ```python
            result = control.icaro_vs_sscc(groupby_cols=['ejercicio', 'mes'])
            ```
        """
        icaro = self.icaro_summarize(groupby_cols=groupby_cols).copy()
        icaro['sellos'] = icaro['sellos'] + icaro['lp']
        icaro = icaro.drop(columns=['lp'])
        icaro = icaro.set_index(groupby_cols)
        sscc = self.sscc_summarize(groupby_cols=groupby_cols).copy()
        sscc = sscc.set_index(groupby_cols)
        # Obtener los índices faltantes en icaro
        missing_indices = sscc.index.difference(icaro.index)
        # Reindexar el DataFrame icaro con los índices faltantes
        icaro = icaro.reindex(icaro.index.union(missing_indices))
        sscc = sscc.reindex(icaro.index)
        icaro = icaro.fillna(0)
        sscc = sscc.fillna(0)
        df = icaro.subtract(sscc)
        df = df.reset_index()
        df = df.fillna(0)
        #Reindexamos el DataFrame
        icaro = icaro.reset_index()
        df = df.reindex(columns=icaro.columns)
        if only_diff:
            # Seleccionar solo las columnas numéricas
            numeric_cols = df.select_dtypes(include=np.number).columns
            # Filtrar el DataFrame utilizando las columnas numéricas válidas
            df = df[df[numeric_cols].sum(axis=1) != 0]
            df = df.reset_index(drop=True)
        return df