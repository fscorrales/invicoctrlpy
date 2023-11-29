#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control Aporte 3% INVICO
Data required:
    - SIIF rci02
    - SIIF rcocc31 (
        1112-2-6 Banco INVICO
        2122-1-2 Retenciones
    )
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
class ControlAporteEmpresario(ImportDataFrame):
    """
    Represents a controller for managing and processing 3% INVICO data.

    Attributes:
        ejercicio (List[str]): List of strings representing the fiscal year.
            Default value is obtained from a factory function (default_ejercicio).
        input_path (str): Path to the input directory for data import. Can be left as None if not provided.
        db_path (str): Path to the directory or location of the database. Can be left as None if not provided.
        update_db (bool): Indicates if a database update should be performed.
            Default is set to False.

    This class is expected to be inherited from a base class 'ImportDataFrame'
    and provides functionalities related to importing, processing, and controlling
    specific 3% INVICO data. Its methods and additional attributes
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

        update_siif = update_db.UpdateSIIF(
            update_path_input + '/Reportes SIIF', 
            self.db_path + '/siif.sqlite')
        update_siif.update_comprobantes_rec_rci02()
        update_siif.update_mayor_contable_rcocc31()


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
    def import_siif_recurso_3_percent(self):
        """
        Import SIIF (Sistema Integrado de Información Financiera) data for 3% resources related to INVICO.

        Returns:
            pd.DataFrame: A DataFrame containing SIIF data specifically related to 3% resources
            associated with INVICO.

        This method imports SIIF data for the specified exercise (year) from the RCI02 table.
        It filters the data to include only records marked as related to INVICO and verified.
        The resulting DataFrame contains information regarding 3% resources related to INVICO.
        """
        df = super().import_siif_rci02(self.ejercicio)
        df = df.loc[df['es_invico'] == True]
        # df = df.loc[df['es_remanente'] == False]
        df = df.loc[df['es_verificado'] == True]
        # keep = ['MACRO']
        # df['cta_cte'].loc[df.glosa.str.contains('|'.join(keep))] = 'Macro'
        # df['grupo'] = np.where(df['cta_cte'] == '10270', 'FONAVI',
        #             np.where(df['cta_cte'].isin([
        #                 "130832-12", "334", "Macro", "Patagonia"]), 'RECUPEROS', 
        #                 'OTROS'))
        df = df.rename(columns={
            'importe': 'recurso_3%',
        })
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def siif_summarize_recurso_3_percent(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:
        """
        Summarize SIIF (Sistema Integrado de Información Financiera) data for 3% resources related to INVICO.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for summarization.
                Defaults to ['ejercicio', 'mes', 'cta_cte'].

        Returns:
            pd.DataFrame: A DataFrame containing summarized SIIF data specifically related to 3%
            resources associated with INVICO.

        This method summarizes SIIF data for the specified exercise (year), focusing on 3% resources
        related to INVICO. It imports and filters the data, grouping it based on the provided columns.
        The resulting DataFrame contains summarized SIIF data for 3% resources linked to INVICO.

        Example:
            To summarize 'siif' data based on custom grouping columns:

            ```python
            siif_summary = control.siif_summarize_recurso_3_percent(
                groupby_cols=['ejercicio', 'mes']
            )
            ```
        """
        siif = self.import_siif_recurso_3_percent().copy()
        siif = siif.drop(['es_remanente', 'es_verificado', 'es_invico'], axis=1)
        siif = siif.groupby(groupby_cols).sum(numeric_only=True)
        siif = siif.reset_index()
        df = siif
        df = df.fillna(0)
        return df


    # --------------------------------------------------
    def import_siif_retencion_337(self) -> pd.DataFrame:
        """
        Import and process SIIF (Sistema Integrado de Información Financiera) data related to code 337 retentions.

        Returns:
            pd.DataFrame: A DataFrame containing processed SIIF data related to retentions under code 337.

        This method retrieves SIIF data associated with specific account codes related to retentions under code 337.
        It processes this data, filtering transactions and merging relevant details, such as financial amounts and
        transaction types. The resulting DataFrame includes processed SIIF data pertinent to retentions under code 337
        for further analysis or usage.
        """
        siif_banco = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '1112-2-6'
        )
        siif_banco = siif_banco.loc[
            siif_banco['tipo_comprobante'] != 'APE', 
            ['ejercicio', 'nro_entrada', 'auxiliar_1']
        ]
        siif_banco = siif_banco.rename(columns={
            'auxiliar_1': 'cta_cte'
        })
        siif_337 = self.import_siif_rcocc31(
            ejercicio = self.ejercicio, cta_contable = '2122-1-2'
        )
        siif_337 = siif_337.loc[siif_337['tipo_comprobante'] != 'APE']
        siif_337 = siif_337.loc[siif_337['auxiliar_1'] == '337']
        siif_337 = siif_337.loc[:, [
            'ejercicio', 'mes', 'fecha', 'nro_entrada', 
            'tipo_comprobante', 'debitos', 'creditos'
        ]]
        siif_337 = siif_337.rename(columns={
            'debitos': 'pagos_337_siif',
            'creditos': 'gastos_337_siif'
        })
        df = siif_337.merge(siif_banco, how='left', on=['ejercicio', 'nro_entrada'])
        map_to = self.ctas_ctes.loc[:,['map_to', 'siif_contabilidad_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='siif_contabilidad_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'siif_contabilidad_cta_cte'], axis='columns', inplace=True)
        return df

    # --------------------------------------------------
    def siif_summarize_retencion_337(
        self, groupby_cols:List[str] = ['ejercicio', 'mes']
    ) -> pd.DataFrame:
        """
        Summarize SIIF (Sistema Integrado de Información Financiera) data related to code 337 retentions.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for summarization.
                Defaults to ['ejercicio', 'mes'].

        Returns:
            pd.DataFrame: A DataFrame containing summarized SIIF data related to code 337 retentions.

        This method fetches SIIF data pertinent to code 337 retentions using the 'import_siif_retencion_337'
        function. It then performs a summarization based on the provided groupby columns. The resulting DataFrame
        presents the summarized SIIF data specifically related to retentions under code 337, organized according
        to the specified grouping for further analysis or utilization.
        """
        siif = self.import_siif_retencion_337().copy()
        siif = siif.groupby(groupby_cols).sum(numeric_only=True)
        siif = siif.reset_index()
        df = siif
        df = df.fillna(0)
        return df

    # --------------------------------------------------
    def siif_recurso_vs_retencion_337(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte'],
        only_diff = False
    ) -> pd.DataFrame:
        """
        Compare SIIF resource data against code 337 retentions.

        Args:
            groupby_cols (List[str], optional): A list of columns to group the data by for comparison.
                Defaults to ['ejercicio', 'mes', 'cta_cte'].
            only_diff (bool, optional): If True, only returns rows with differences between SIIF resource
                and code 337 retentions data. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing a comparison between SIIF resource data and code 337
            retentions data.

        This method compares SIIF resource data, summarized using 'siif_summarize_recurso_3_percent',
        with code 337 retention data, summarized using 'siif_summarize_retencion_337'. It aligns both datasets
        based on the provided groupby columns and produces a DataFrame highlighting the differences between the
        two datasets when 'only_diff' is True. If 'only_diff' is False, it returns a DataFrame with the merged
        data, suitable for further analysis or review.
        """
        siif_recurso = self.siif_summarize_recurso_3_percent(groupby_cols=groupby_cols).copy()
        siif_recurso = siif_recurso.set_index(groupby_cols)
        siif_retencion = self.siif_summarize_retencion_337(groupby_cols=groupby_cols).copy()
        siif_retencion.drop(['gastos_337_siif'], axis='columns', inplace=True)
        siif_retencion = siif_retencion.set_index(groupby_cols)
        # Obtener los índices faltantes en siif
        missing_indices = siif_retencion.index.difference(siif_recurso.index)
        # Reindexar el DataFrame siif con los índices faltantes
        siif_recurso = siif_recurso.reindex(siif_recurso.index.union(missing_indices))
        siif_retencion = siif_retencion.reindex(siif_recurso.index)
        siif_recurso = siif_recurso.fillna(0)
        siif_retencion = siif_retencion.fillna(0)
        df = siif_recurso.merge(siif_retencion, how='outer', on=groupby_cols)
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