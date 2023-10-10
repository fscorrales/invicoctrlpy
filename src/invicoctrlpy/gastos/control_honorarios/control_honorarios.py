#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
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
from dataclasses import dataclass, field
from typing import List

import numpy as np
import pandas as pd
from invicoctrlpy.utils.import_dataframe import ImportDataFrame
from invicodb.update import update_db


def default_ejercicio():
    return [str(dt.datetime.now().year)]

@dataclass
# --------------------------------------------------
class ControlHonorarios(ImportDataFrame):
    """
    ControlHonorarios is a Python class for managing and analyzing financial data related to honorariums.

    This class inherits from the ImportDataFrame class and provides functionality to update SQL databases,
    import relevant data, and perform various data analysis tasks.

    Attributes:
        ejercicio (List[str]): A list of fiscal years as strings. Default is the
            current year as a single-element list.
        db_path (str): The path to the SQL database.
        update_db (bool): A flag indicating whether to update the database during initialization.

    Methods:
        __post_init__(): Initializes the ControlHonorarios object, updates the database if required,
            and imports necessary DataFrames.
        update_sql_db(): Updates the SQL database by migrating data from external sources.
        import_dfs(): Imports DataFrames.
        import_slave(): Imports data related to a "slave" system and performs data transformations.
        slave_summarize(): Summarizes data related to the "slave" system, allowing for customization.
        import_siif_comprobantes(): Imports SIIF (Sistema Integrado de Información Financiera) data,
            applying specific filters.
        siif_summarize(): Summarizes SIIF data, allowing for grouping and aggregation.
        import_resumen_rend_honorarios(): Imports data related to resumen de rendiciones de honorarios.
        sgf_summarize(): Summarizes SGF (Sistema de Gestión Financiera) data, with customization options.
        siif_vs_slave(): Compares data between SIIF and the "slave" system, identifying differences.
        slave_vs_sgf(): Compares data between the "slave" system and SGF, with filtering and diff options.
    """
    ejercicio: List[str] = field(default_factory=default_ejercicio)
    input_path:str = None
    db_path:str = None
    update_db:bool = False

    # --------------------------------------------------
    def __post_init__(self):
        """
        Initializes the ControlHonorarios class.

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

        update_slave = update_db.UpdateSlave(
            update_path_input + '/Slave/Slave.mdb', 
            self.db_path + '/slave.sqlite')
        update_slave.migrate_slave()

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
    def import_slave(self) -> pd.DataFrame:
        """
        Import financial data related to the "slave" system and perform data preprocessing.

        This method imports financial data related to the "slave" system, applies filtering and data transformations,
        and returns a processed DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing processed financial data from the "slave" system.

        Notes:
            - Data filtering: Excludes rows with 'nro_comprobante' containing certain keywords.
            - Data integration: Merges 'cta_cte' information from the SIIF (Sistema Integrado de Información Financiera).
            - Data renaming: Renames the 'razon_social' column to 'beneficiario'.
            - Data column removal: Drops the 'id' column from the DataFrame.
            - Missing values: Fills missing values with 0.
        """
        df = super().import_slave(ejercicio = self.ejercicio)
        keep = ['SIIF']
        df = df.loc[~df.nro_comprobante.str.contains('|'.join(keep))]
        cta_cte = self.import_siif_rcg01_uejp(self.ejercicio)
        cta_cte = cta_cte.loc[:, ['nro_comprobante', 'cta_cte']]
        df = df.merge(cta_cte, on='nro_comprobante', how='left')
        df = df.fillna(0)
        df = df.rename(columns={'razon_social': 'beneficiario'})
        df = df.drop(columns=['id'])
        df = df.reset_index(drop=True)
        # df['origen'] = df['cta_cte'].apply(lambda x: 'FUNC' if x == '130832-05' else 'EPAM')
        return df

    # --------------------------------------------------
    def slave_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'nro_comprobante', 'cta_cte'],
        only_importe_bruto = False
    ) -> pd.DataFrame:
        """
        Summarize financial data from the "slave" system.

        This method summarizes financial data from the "slave" system by performing aggregation based on the specified
        grouping columns. It returns a DataFrame containing the summarized data.

        Args:
            groupby_cols (List[str], optional): List of column names to group the data by. Defaults to
                ['ejercicio', 'mes', 'nro_comprobante', 'cta_cte'].
            only_importe_bruto (bool, optional): If True, includes only the 'importe_bruto' column in the resulting DataFrame.
                Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing summarized financial data from the "slave" system.

        Notes:
            - Data aggregation: Aggregates financial data based on the specified grouping columns.
            - Optional column selection: Allows including only the 'importe_bruto' column if 'only_importe_bruto' is True.
            - Missing values: Fills missing values with 0.
        """
        slave = self.import_slave().copy()
        slave = slave.rename(columns={'otras_retenciones': 'otras'})
        slave['otras'] = slave['otras'] + slave['anticipo'] + slave['descuento'] + slave['embargo'] + slave['mutual']
        slave['sellos'] = slave['sellos'] + slave['lp'] 
        slave = slave.drop(columns=['anticipo', 'descuento', 'embargo', 'lp', 'mutual'])
        if only_importe_bruto:
            slave = slave.loc[:, groupby_cols + ['importe_bruto']]
        slave = slave.groupby(groupby_cols).sum(numeric_only=True)
        slave = slave.reset_index()
        df = slave
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    def import_siif_comprobantes(self):
        """
        Import and filter SIIF comprobante data.

        This method imports comprobante data from the SIIF (Sistema Integrado de Información Financiera) system and filters
        it based on specific criteria. It returns a DataFrame containing the filtered comprobante data.

        Returns:
            pd.DataFrame: A DataFrame containing filtered SIIF comprobante data.

        Notes:
            - Data import: Imports comprobante data from the SIIF system for the current year (self.ejercicio).
            - Data filtering: Filters the imported data based on specific criteria, including CUIT, grupo, partida, cta_cte,
            and glosa values.
            - Reset index: Resets the DataFrame index for consistency.
        """
        df = super().import_siif_comprobantes(ejercicio = self.ejercicio)
        df = df.loc[df['cuit'] == '30632351514']
        df = df.loc[df['grupo'] == '300']
        df = df.loc[df['partida'] != '384']
        df = df.loc[df['cta_cte'].isin(['130832-05', '130832-07'])]
        keep = ['HONOR', 'RECON', 'LOC']
        df = df.loc[df.glosa.str.contains('|'.join(keep))]
        df = df.reset_index(drop=True)
        # df['origen'] = df['cta_cte'].apply(lambda x: 'FUNC' if x == '130832-05' else 'EPAM')
        return df

    # --------------------------------------------------
    def siif_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'nro_comprobante', 'cta_cte']
    ) -> pd.DataFrame:
        """
        Summarize and aggregate SIIF comprobante data.

        This method imports SIIF comprobante data using the `import_siif_comprobantes` method and summarizes it by aggregating
        it based on the specified grouping columns. It returns a DataFrame containing the summarized data.

        Args:
            groupby_cols (List[str], optional): A list of column names used for grouping and summarizing the data.
                Defaults to ['ejercicio', 'mes', 'nro_comprobante', 'cta_cte'].

        Returns:
            pd.DataFrame: A DataFrame containing summarized SIIF comprobante data.

        Notes:
            - Data import: Imports comprobante data from the SIIF system using the `import_siif_comprobantes` method.
            - Data aggregation: Aggregates the imported data by grouping it based on the specified columns and calculating
            the sum of numeric columns.
            - Reset index: Resets the DataFrame index for consistency.
        """
        siif = self.import_siif_comprobantes().copy()
        # siif = siif.loc[:, ['nro_comprobante', 'importe', 'mes', 'cta_cte']]
        siif = siif.groupby(groupby_cols).sum(numeric_only=True)
        siif = siif.reset_index()
        df = siif
        df = df.fillna(0)
        return df

        # --------------------------------------------------
    def import_resumen_rend_honorarios(self) -> pd.DataFrame:
        """
        Import summary of honorarios renditions.

        This method imports a summary of honorarios renditions data for the specified fiscal year (ejercicio). It returns
        the data as a DataFrame.

        Args:
            None

        Returns:
            pd.DataFrame: A DataFrame containing the summary of honorarios renditions data for the specified fiscal year.

        Notes:
            - Data import: Imports the summary data of honorarios renditions using the `import_resumen_rend_honorarios`
            method from the superclass.
            - DataFrame transformation: Resets the index of the imported DataFrame for consistency.
        """
        df = super().import_resumen_rend_honorarios(
            ejercicio = self.ejercicio
        )
        df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def sgf_summarize(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cuit', 'beneficiario'], 
        only_importe_bruto = False
    ) -> pd.DataFrame:
        """
        Summarize SGF (Sistema de Gestión Financiera) data.

        This method summarizes financial data from SGF for specific grouping columns. It returns the summarized data as
        a DataFrame.

        Args:
            groupby_cols (List[str], optional): A list of column names to group the data by. Defaults to 
                ['ejercicio', 'mes', 'cuit', 'beneficiario'].
            only_importe_bruto (bool, optional): If True, only the 'importe_bruto' column is included in the resulting
                DataFrame. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing the summarized SGF financial data based on the specified grouping columns.

        Notes:
            - Data import: Imports financial data from SGF using the `import_resumen_rend_honorarios` method from the
            superclass.
            - DataFrame transformation: Groups the data by the specified columns and calculates the sum for numeric
            columns. If 'only_importe_bruto' is True, the DataFrame is filtered to include only 'importe_bruto'.
        """
        df = self.import_resumen_rend_honorarios().copy()
        df['otras'] = df['otras'] + df['gcias'] + df['suss'] + df['invico'] + df['salud'] + df['mutual']
        df = df.drop(['gcias', 'suss', 'invico', 'salud', 'mutual'], axis=1)
        if only_importe_bruto:
            df = df.loc[:, groupby_cols + ['importe_bruto']]
        df = df.groupby(groupby_cols).sum(numeric_only=True)
        df = df.reset_index()
        return df

    # --------------------------------------------------
    def siif_vs_slave(self) -> pd.DataFrame:
        """
        Compare SIIF (Sistema Integrado de Información Financiera) and Slave financial data.

        This method performs a detailed comparison between financial data from SIIF and Slave databases. It identifies
        discrepancies in financial transactions and reports them.

        Returns:
            pd.DataFrame: A DataFrame containing the comparison results, including discrepancies in transaction numbers
            ('nro'), transaction amounts ('importe'), account codes ('cta_cte'), and months ('mes').

        Notes:
            - Data import: Imports financial data from both SIIF and Slave databases using the 'siif_summarize' and
            'slave_summarize' methods.
            - Data comparison: Compares the financial data based on transaction numbers, transaction amounts, account
            codes, and months. Discrepancies are identified and reported in the resulting DataFrame.
            - Filtering: The resulting DataFrame only includes rows with discrepancies in any of the compared columns.
            - Sorting: The DataFrame is sorted in descending order based on the severity of discrepancies
            ('err_nro', 'err_importe', 'err_cta_cte', 'err_mes').
        """
        groupby_cols = ['ejercicio', 'mes', 'nro_comprobante', 'cta_cte']
        siif = self.siif_summarize(groupby_cols = groupby_cols).copy()
        siif = siif.rename(columns={
            'importe':'siif_importe',
            'nro_comprobante':'siif_nro',
            'cta_cte':'siif_cta_cte',
            'mes':'siif_mes'
        })
        slave = self.slave_summarize(
            groupby_cols = groupby_cols, 
            only_importe_bruto=True
        ).copy()
        slave = slave.rename(columns={
            'importe_bruto':'slave_importe',
            'nro_comprobante':'slave_nro',
            'cta_cte':'slave_cta_cte',
            'mes':'slave_mes'
        })
        df = pd.merge(siif, slave, how='outer', 
        left_on=['ejercicio','siif_nro'], 
        right_on=['ejercicio','slave_nro'], copy=False)
        df = df.fillna(0)
        df['err_nro'] = df['siif_nro'] != df['slave_nro']
        df['err_importe'] = ~np.isclose(df['siif_importe'] - df['slave_importe'], 0)
        df['err_cta_cte'] = df['siif_cta_cte'] != df['slave_cta_cte']
        df['err_mes'] = df['siif_mes'] != df['slave_mes']
        df = df.loc[:, [
            'ejercicio',
            'siif_nro', 'slave_nro', 'err_nro', 
            'siif_importe', 'slave_importe', 'err_importe', 
            'siif_mes', 'slave_mes', 'err_mes', 
            'siif_cta_cte', 'slave_cta_cte', 'err_cta_cte'
        ]]
        df = df.query('err_nro | err_mes | err_importe | err_cta_cte')
        df = df.sort_values(
            by=[
                'err_nro', 'err_importe', 
                'err_cta_cte', 'err_mes'
            ], 
            ascending=False
        )
        df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def slave_vs_sgf(
        self, groupby_cols:List[str] = [
            'ejercicio', 'mes',
            'cta_cte', 'beneficiario'
        ],
        only_importe_bruto = False, only_diff = False
    ) -> pd.DataFrame:
        """
        Compare Slave and SGF (Sistema de Gestión Financiera) financial data.

        This method performs a detailed comparison between financial data from the Slave and SGF databases. It identifies
        discrepancies in financial transactions and reports them.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for the comparison.
            only_importe_bruto (bool, optional): If True, only the 'importe_bruto' column will be considered for the
                comparison. Default is False.
            only_diff (bool, optional): If True, only rows with discrepancies will be included in the resulting DataFrame.
                Default is False.

        Returns:
            pd.DataFrame: A DataFrame containing the comparison results, including discrepancies in transaction numbers
            ('nro_comprobante'), transaction amounts ('importe_bruto'), account codes ('cta_cte'), and beneficiaries
            ('beneficiario').

        Notes:
            - Data import: Imports financial data from both the Slave and SGF databases using the 'slave_summarize' and
            'sgf_summarize' methods.
            - Data comparison: Compares the financial data based on the specified columns. Discrepancies are identified
            and reported in the resulting DataFrame.
            - Filtering: The resulting DataFrame can be filtered to include only rows with discrepancies if 'only_diff'
            is set to True.
        """
        slave = self.slave_summarize(
            groupby_cols = groupby_cols, only_importe_bruto=only_importe_bruto
        ).copy()
        sgf = self.sgf_summarize(
            groupby_cols = groupby_cols, only_importe_bruto=only_importe_bruto
        ).copy()
        slave = slave.set_index(groupby_cols)
        sgf = sgf.set_index(groupby_cols)
        df = slave.subtract(sgf)
        df = df.reset_index()
        df = df.fillna(0)
        #Reindexamos el DataFrame
        slave = slave.reset_index()
        df = df.reindex(columns=slave.columns)
        if only_diff:
            # Seleccionar solo las columnas numéricas
            numeric_cols = df.select_dtypes(include=np.number).columns
            # Filtrar el DataFrame utilizando las columnas numéricas válidas
            # df = df[df[numeric_cols].sum(axis=1) != 0]
            df = df[~np.isclose(df[numeric_cols].sum(axis=1), 0)]
        return df