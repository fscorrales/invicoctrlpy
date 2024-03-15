#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control de Banco SIIF vs Real INVICO (SSCC)
Data required:
    - SSCC Resumen General de Movimientos
    - SSCC Saldo Final
    - SSCC ctas_ctes (manual data)
    - SIIF rcocc31 (1112-2-6 Banco)
    - SIIF rfondo07tp (PA6)
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
        update_siif.update_resumen_fdos_rfondo07tp()

        update_sscc = update_db.UpdateSSCC(
            update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
            self.db_path + '/sscc.sqlite')
        update_sscc.update_ctas_ctes()
        update_sscc.update_sdo_final_banco_invico()
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
        self.control_recursos = ControlRecursos(ejercicio=self.ejercicio, update_db=self.update_db)
        self.control_obras = ControlObras(ejercicio=self.ejercicio, update_db=self.update_db)
        self.control_haberes = ControlHaberes(ejercicio=self.ejercicio, update_db=self.update_db)
        self.control_honorarios = ControlHonorarios(ejercicio=self.ejercicio, update_db=self.update_db)
        self.control_debitos_bancarios = ControlDebitosBancarios(ejercicio=self.ejercicio, update_db=self.update_db)
        self.control_escribanos = ControlEscribanos(ejercicio=self.ejercicio, update_db=self.update_db)
        self.import_ctas_ctes()

    # --------------------------------------------------
    def banco_siif(self) -> pd.DataFrame:
 
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

        banco_siif = self.banco_siif().copy()
        banco_siif = banco_siif.groupby(groupby_cols).sum(numeric_only=True)
        banco_siif = banco_siif.reset_index()
        df = banco_siif
        df = df.fillna(0)
        return df

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

    # --------------------------------------------------
    def banco_invico(self) -> pd.DataFrame:
        """
        Imports Banco Invico data for he specified fiscal year.

        Args:
            None

        Returns:
            pd.DataFrame: Pandas DataFrame containing Banco Invico data.
        """
        return super().import_banco_invico(ejercicio=self.ejercicio)

    # --------------------------------------------------
    def banco_invico_saldo_acum(
        self, groupby_cols:List[str] = ['ejercicio', 'mes', 'cta_cte']
    ) -> pd.DataFrame:
        df = super().import_banco_invico(ejercicio=None)
        df = df.loc[df['ejercicio'].astype(int) <= int(self.ejercicio[-1])]
        df = df.sort_values(by=['fecha'], ascending=True)
        df['saldo'] = df.groupby('cta_cte')['importe'].cumsum()
        df = df.loc[:,groupby_cols + ['saldo']]
        df = df.fillna(0)
        df = df.groupby(groupby_cols).last().reset_index()

        # df = df.drop('importe', axis=1)
        # if isinstance(self.ejercicio, list):
        #     df = df.loc[df['ejercicio'].isin(self.ejercicio)]
        # else:
        #     df = df.loc[df['ejercicio'].isin([self.ejercicio])]
        # df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def banco_invico_sldo_final_vs_acum(
        self, groupby_cols:List[str] = ['ejercicio', 'cta_cte'],
        only_diff = False
    ) -> pd.DataFrame:
        sldo_final = self.banco_invico_saldo_final().copy()
        sldo_final = sldo_final.rename(columns={
            'saldo': 'sldo_final'
        })
        sldo_final = sldo_final.set_index(groupby_cols)
        sldo_acum = self.banco_invico_saldo_acum(groupby_cols=groupby_cols).copy()
        sldo_acum = sldo_acum.rename(columns={
            'saldo': 'sldo_acum'
        })
        sldo_acum = sldo_acum.set_index(groupby_cols)
        # Obtener los índices faltantes en icaro
        missing_indices = sldo_acum.index.difference(sldo_final.index)
        # Reindexar el DataFrame icaro con los índices faltantes
        sldo_final = sldo_final.reindex(sldo_final.index.union(missing_indices))
        sldo_acum = sldo_acum.reindex(sldo_final.index)
        sldo_final = sldo_final.fillna(0)
        sldo_acum = sldo_acum.fillna(0)
        df = pd.merge(sldo_final, sldo_acum, how='inner', on=groupby_cols)
        # df = sldo_final.subtract(sldo_acum)
        df = df.reset_index()
        df = df.fillna(0)
        df['dif'] = df['sldo_final'] - df['sldo_acum']
        #Reindexamos el DataFrame
        if only_diff:
            # Seleccionar solo las columnas numéricas
            numeric_cols = df.select_dtypes(include=np.number).columns
            # Filtrar el DataFrame utilizando las columnas numéricas válidas
            df = df[df[numeric_cols].sum(axis=1) != 0]
            df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def banco_siif_vs_invico_sldo_final(
        self, groupby_cols:List[str] = ['ejercicio', 'cta_cte'],
        only_diff = False
    ) -> pd.DataFrame:
        banco_invico = self.banco_invico_saldo_final().copy()
        banco_invico = banco_invico.loc[:, groupby_cols + ['saldo']]
        banco_invico = banco_invico.rename(columns={
            'saldo': 'sldo_invico'
        })
        banco_invico = banco_invico.set_index(groupby_cols)
        banco_siif = self.banco_siif_summarize(groupby_cols=groupby_cols).copy()
        banco_siif = banco_siif.rename(columns={
            'saldo': 'sldo_siif'
        })
        banco_siif = banco_siif.set_index(groupby_cols)
        # Obtener los índices faltantes en icaro
        missing_indices = banco_siif.index.difference(banco_invico.index)
        # Reindexar el DataFrame icaro con los índices faltantes
        banco_invico = banco_invico.reindex(banco_invico.index.union(missing_indices))
        banco_siif = banco_siif.reindex(banco_invico.index)
        banco_invico = banco_invico.fillna(0)
        banco_siif = banco_siif.fillna(0)
        df = pd.merge(banco_siif, banco_invico, how='inner', on=groupby_cols)
        # df = sldo_final.subtract(sldo_acum)
        df = df.reset_index()
        df = df.fillna(0)
        df['dif_sldo'] = df['sldo_siif'] - df['sldo_invico']
        #Reindexamos el DataFrame
        if only_diff:
            # Seleccionar solo las columnas numéricas
            numeric_cols = df.select_dtypes(include=np.number).columns
            # Filtrar el DataFrame utilizando las columnas numéricas válidas
            df = df[df[numeric_cols].sum(axis=1) != 0]
            df = df.reset_index(drop=True)
        return df

    # --------------------------------------------------
    def banco_siif_vs_invico_ajustes(
        self, incluir_pa6 = True, incluir_honorarios = True, incluir_escribanos = True
    ) -> pd.DataFrame:
        df = self.banco_siif_vs_invico_sldo_final().copy()
        df = df.loc[:, ['ejercicio','sldo_siif', 'sldo_invico', 'dif_sldo']]
        df = df.groupby(['ejercicio']).sum()
        df = df.reset_index()
        recursos = self.control_recursos_siif().copy()
        recursos = recursos.loc[:, ['ejercicio', 'diferencia']]
        recursos = recursos.groupby(['ejercicio']).sum(numeric_only=True)
        recursos['diferencia'] = recursos['diferencia'] * -1
        recursos = recursos.rename(columns={'diferencia':'recursos_dif'})
        obras = self.control_obras_siif().copy()
        obras = obras.loc[:, ['ejercicio', 'diferencia']]
        obras = obras.groupby(['ejercicio']).sum(numeric_only=True)
        obras = obras.rename(columns={'diferencia':'obras_dif'})
        haberes = self.control_haberes_siif().copy()
        haberes = haberes.loc[:, ['ejercicio', 'diferencia']]
        haberes = haberes.groupby(['ejercicio']).sum(numeric_only=True)
        haberes = haberes.rename(columns={'diferencia':'haberes_dif'})
        debitos_bancarios = self.control_debitos_bancarios_siif().copy()
        debitos_bancarios = debitos_bancarios.loc[:, ['ejercicio', 'diferencia']]
        debitos_bancarios = debitos_bancarios.groupby(['ejercicio']).sum(numeric_only=True)
        debitos_bancarios = debitos_bancarios.rename(columns={'diferencia':'debitos_banca_dif'})

        # Merge
        df = df.merge(recursos, how='left', on='ejercicio', copy=False)
        df = df.merge(obras, how='left', on='ejercicio', copy=False)
        df = df.merge(haberes, how='left', on='ejercicio', copy=False)
        df = df.merge(debitos_bancarios, how='left', on='ejercicio', copy=False)
        if incluir_pa6:
            pa6 = self.pa6_siif().copy()
            pa6 = pa6.loc[:, ['ejercicio', 'egresos']]
            pa6 = pa6.groupby(['ejercicio']).sum(numeric_only=True)
            pa6 = pa6.rename(columns={'egresos':'pa6_reg'})
            df = df.merge(pa6, how='left', on='ejercicio', copy=False)

        if incluir_honorarios:
            honorarios = self.control_honorarios_siif().copy()
            honorarios = honorarios.loc[:, ['ejercicio', 'importe_bruto']]
            honorarios = honorarios.groupby(['ejercicio']).sum(numeric_only=True)
            honorarios = honorarios.rename(columns={'importe_bruto':'honorarios_dif'})
            df = df.merge(honorarios, how='left', on='ejercicio', copy=False)

        if incluir_escribanos:
            escribanos = self.control_escribanos_siif().copy()
            escribanos = escribanos.loc[:, ['ejercicio', 'dif_pagos']]
            escribanos = escribanos.groupby(['ejercicio']).sum(numeric_only=True)
            escribanos = escribanos.rename(columns={'dif_pagos':'escribanos_dif'})
            df = df.merge(escribanos, how='left', on='ejercicio', copy=False)

        df['dif_sldo_ajustado'] = (
            df['dif_sldo'] + df['recursos_dif'] + df['obras_dif'] + 
            df['haberes_dif'] + df['debitos_banca_dif']
        )
        if incluir_honorarios:
            df['dif_sldo_ajustado'] = df['dif_sldo_ajustado'] + df['honorarios_dif']
        if incluir_escribanos:
            df['dif_sldo_ajustado'] = df['dif_sldo_ajustado'] + df['escribanos_dif']
        if incluir_pa6:
            df['dif_sldo_ajustado'] = df['dif_sldo_ajustado'] + df['pa6_reg']

        df = df.reset_index(drop=True)
        # # Transformar el DataFrame
        df = df.melt(id_vars='ejercicio', var_name='concepto', value_name='Importe')
        
        return df