#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@hotmail.com>
Purpose: Control de Retenciones Banco INVICO vs SIIF o Icaro
Data required:
    - Icaro
    - SIIF rdeu012 (para netear Icaro)
    - SGF Resumen Rend por Proveedor
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
    - SIIF rcocc31 (
        2111-1-2 Contratistas
        2122-1-2 Retenciones
        1112-2-6 Banco)
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
class ControlRetenciones(ImportDataFrame):
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

        update_icaro = update_db.UpdateIcaro(
            os.path.dirname(os.path.dirname(self.db_path)) + 
            '/R Output/SQLite Files/ICARO.sqlite', 
            self.db_path + '/icaro.sqlite')
        update_icaro.migrate_icaro()

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_mayor_contable_rcocc31()
        update_siif.update_deuda_flotante_rdeu012()

        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_banco_invico()

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
        self.import_icaro_carga_neto_rdeu()
        # self.siif_desc_pres = self.import_siif_desc_pres(ejercicio_to=self.ejercicio)
        # self.icaro_desc_pres = self.import_icaro_desc_pres()

    # --------------------------------------------------
    def import_icaro_carga_neto_rdeu(self):
        """
        Imports net "Icaro Carga" data and calculates net data for the specified fiscal year.

        Args:
            None

        Returns:
            pd.DataFrame: Pandas DataFrame containing net "Icaro Carga" net data.
        """
        self.icaro_carga = super().import_icaro_carga_neto_rdeu(ejercicio=self.ejercicio)
        return self.icaro_carga

    # --------------------------------------------------
    def import_icaro_retenciones(self) -> pd.DataFrame:
        """
        Imports "Icaro Retention" data and performs necessary data manipulation
        to aggregate and calculate total retentions.

        Args:
            None

        Returns:
            pd.DataFrame: Pandas DataFrame containing aggregated retention data.
        """
        icaro_carga = self.icaro_carga.copy()
        icaro_retenciones = super().import_icaro_retenciones()
        icaro_retenciones = icaro_retenciones.loc[
            icaro_retenciones['id_carga'].isin(icaro_carga['id'].values.tolist())]
        icaro_retenciones.reset_index(drop=True, inplace=True)
        icaro_retenciones = icaro_retenciones.pivot_table(
            index = 'id_carga', columns='codigo', values='importe', 
            aggfunc='sum', fill_value=0
        )
        icaro_retenciones = icaro_retenciones.reset_index()
        icaro_retenciones = icaro_retenciones.rename_axis(None, axis=1)
        icaro_retenciones['retenciones'] = icaro_retenciones.sum(axis=1, numeric_only= True)
        return icaro_retenciones

    # --------------------------------------------------
    def import_icaro_carga_con_retenciones(self) -> pd.DataFrame:
        """
        Imports "Icaro Carga" data and associated retentions, performing
        calculations to determine net amounts.

        Args:
            None

        Returns:
            pd.DataFrame: Pandas DataFrame containing "Icaro Carga" data
            with retentions calculated.
        """
        icaro_carga = self.icaro_carga.copy()
        icaro_retenciones = self.import_icaro_retenciones()
        df = icaro_carga.merge(
            icaro_retenciones, how='left', 
            left_on='id', right_on='id_carga', copy=False
        )
        df = df.fillna(0)
        df['importe_bruto'] = df['importe']
        df['retenciones'] = df.apply(lambda row: row['retenciones'] * (-1) if row['importe_bruto'] < 0 else row['retenciones'], axis=1)
        df['importe_neto'] = df['importe_bruto'] - df['retenciones']
        df = df.drop(
            ['importe', 'id_carga', 'nro_comprobante'], 
            axis=1
        )
        if '113' in df.columns:
            df = df.rename(columns={'113': 'gcias'})
        if '111' in df.columns:
            df = df.rename(columns={'111': 'sellos'})
        if '112' in df.columns:
            df = df.rename(columns={'112': 'lp'})
        if '110' in df.columns:
            df = df.rename(columns={'110': 'iibb'})
        if '114' in df.columns:
            df = df.rename(columns={'114': 'suss'})
        if '337' in df.columns:
            df = df.rename(columns={'337': 'invico'})
        return df

    # --------------------------------------------------
    def icaro_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:
        """
        Perform cross-control of data.

        Performs cross-control of the imported data by grouping it based on
        the specified columns and summing the values within each group.

        Args:
            groupby_cols (List[str], optional): A list of column names to group and
                perform the summary on. Default is ['ejercicio', 'mes', 'cta_cte'].

        Returns:
            pd.DataFrame: Pandas DataFrame containing the cross-controlled data.
        """
        df = self.import_icaro_carga_con_retenciones().copy()
        df = df.drop(
            ['fondo_reparo', 'avance', 'certificado', 'origen', 'obra'], 
            axis=1
        )
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        return df

    # --------------------------------------------------
    def import_siif_pagos_contratistas(self) -> pd.DataFrame:
        """
        Import payments to contractors from 'siif' data.

        This method imports payment data to contractors from 'siif' (Sistema de
        Información Integrado de Finanzas) based on the specified fiscal year. It
        retrieves data for both bank accounts and contractor payments, renames columns,
        and merges the data to create a comprehensive DataFrame.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing payment data to contractors.

        Example:
            To import payment data for contractors from 'siif' for 
            the specified fiscal year:

            ```python
            contractor_payments = control.import_siif_pagos_contratistas()
            ```
        """
        siif_banco = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '1112-2-6'
        )
        siif_banco = siif_banco.loc[:, ['nro_entrada', 'auxiliar_1']]
        siif_banco = siif_banco.rename(columns={
            'auxiliar_1': 'cta_cte'
        })
        siif_contratistas = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '2111-1-2'
        )
        siif_contratistas = siif_contratistas.loc[
            siif_contratistas['tipo_comprobante'].isin(['CAP', 'ANP', 'CAD'])]
        siif_contratistas = siif_contratistas.loc[:, [
            'ejercicio', 'mes', 'nro_entrada', 
            'tipo_comprobante', 'debitos', 'auxiliar_1'
        ]]
        siif_contratistas = siif_contratistas.rename(columns={
            'debitos': 'importe_neto',
            'auxiliar_1': 'cuit'
        })
        df = siif_contratistas.merge(siif_banco, how='left', on='nro_entrada')
        return df

    # --------------------------------------------------
    def import_siif_pagos_retenciones(self):
        """
        Import payments related to retentions from 'siif' data.

        This method imports payment data related to retentions from 'siif' (Sistema de
        Información Integrado de Finanzas) based on the specified fiscal year. It retrieves
        data for both bank accounts and retention payments, renames columns, and calculates
        the total payments for each retention category.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing payment data related to retentions.

        Example:
            To import payment data for retentions from 'siif' for the current fiscal year:

            ```python
            retention_payments = control.import_siif_pagos_retenciones()
            ```
        """
        siif_banco = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '1112-2-6'
        )
        siif_banco = siif_banco.loc[:, ['nro_entrada', 'auxiliar_1']]
        siif_banco = siif_banco.rename(columns={
            'auxiliar_1': 'cta_cte'
        })
        siif_retenciones = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '2122-1-2'
        )
        siif_retenciones = siif_retenciones.loc[
            siif_retenciones['tipo_comprobante'].isin(['CAP', 'ANP', 'CAD'])]
        siif_retenciones = siif_retenciones.loc[:, [
            'ejercicio', 'mes', 'nro_entrada', 
            'tipo_comprobante', 'debitos', 'auxiliar_1'
        ]]
        siif_retenciones = siif_retenciones.rename(columns={
            'debitos': 'importe',
            'auxiliar_1': 'cod_ret'
        })
        df = siif_retenciones.merge(siif_banco, how='left', on='nro_entrada')
        retenciones_obras = ['110', '111', '112', '113', '114', '337']
        df = df.loc[df['cod_ret'].isin(retenciones_obras)]
        df['cod_ret'] = np.select(
            [
                df['cod_ret'] == '110',
                df['cod_ret'] == '111',
                df['cod_ret'] == '112',
                df['cod_ret'] == '113',
                df['cod_ret'] == '114',
                df['cod_ret'] == '337'
            ],
            ['iibb', 'sellos', 'lp', 'gcias', 'suss', 'invico']
        )
        return df

    # --------------------------------------------------
    def siif_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:
        """
        Summarize 'siif' (Sistema de Información Integrado de Finanzas) data.

        This method summarizes 'siif' data for both contractor payments and retentions
        based on the specified grouping columns. It calculates the total payments and
        retentions for each group and computes the gross amount by adding the retentions
        to the net amount.

        Args:
            groupby_cols (List[str], optional): A list of column names to group and
                perform the summary on. Default is ['ejercicio', 'mes', 'cta_cte'].

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the summarized 'siif' data.

        Example:
            To summarize 'siif' data based on custom grouping columns:

            ```python
            siif_summary = control.siif_summarize(groupby_cols=['ejercicio', 'mes'])
            ```
        """
        contratistas = self.import_siif_pagos_contratistas().copy()
        contratistas = contratistas.groupby(groupby_cols).sum(numeric_only=True)
        contratistas = contratistas.reset_index()
        retenciones = self.import_siif_pagos_retenciones().copy()
        retenciones.reset_index(drop=True, inplace=True)
        retenciones = retenciones.pivot_table(
            index = groupby_cols, columns='cod_ret', values='importe', 
            aggfunc='sum', fill_value=0
        )
        retenciones = retenciones.reset_index()
        retenciones = retenciones.rename_axis(None, axis=1)
        retenciones['retenciones'] = retenciones.sum(axis=1, numeric_only= True)
        df = contratistas.merge(
            retenciones, how='outer', on=groupby_cols
        )
        df = df.fillna(0)
        df['importe_bruto'] = df['importe_neto'] + df['retenciones']
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
        siif = self.siif_summarize(groupby_cols=groupby_cols).copy()
        siif = siif.set_index(groupby_cols)
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
    def sgf_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:
        """
        Perform cross-control of SGF's data.

        Performs cross-control of the imported data by grouping it based on
        the specified columns and summing the values within each group.

        Args:
            groupby_cols (list): List of column names to group by.

        Returns:
            pd.DataFrame: Pandas DataFrame containing the cross-controlled data.
        """
        df = self.import_resumen_rend_cuit().copy()
        df = df.drop(
            ['origen', 'fecha', 'beneficiario', 'destino', 'libramiento_sgf',
            'seguro', 'salud', 'mutual', 'otras'], 
            axis=1
        )
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        return df

    # --------------------------------------------------
    def import_banco_invico(self) -> pd.DataFrame:
        """
        Imports Banco Invico data for he specified fiscal year.

        Args:
            None

        Returns:
            pd.DataFrame: Pandas DataFrame containing Banco Invico data.
        """
        return super().import_banco_invico(ejercicio = self.ejercicio)

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
        return df

    # --------------------------------------------------
    def icaro_vs_invico(
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
        return df