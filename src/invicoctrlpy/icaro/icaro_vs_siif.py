#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: Icaro vs SIIF budget execution
Data required:
    - Icaro
    - SIIF rf602
    - SIIF gto_rpa03g
    - SIIF rcg01_uejp
    - SSCC ctas_ctes (manual data)
Packages:
 - invicodatpy (pip install '/home/kanou/IT/R Apps/R Gestion INVICO/invicodatpy')
"""

import argparse
import datetime as dt
import inspect
import os
from dataclasses import dataclass, field

import pandas as pd
from datar import base, dplyr, f, tidyr
from invicodatpy.icaro.migrate_icaro import MigrateIcaro
from invicodatpy.siif.ppto_gtos_fte_rf602 import PptoGtosFteRf602
from invicodatpy.siif.join_comprobantes_gtos_gpo_part import JoinComprobantesGtosGpoPart
from invicodatpy.sscc.ctas_ctes import CtasCtes


@dataclass
# --------------------------------------------------
class IcaroVsSIIF():
    ejercicio:str = str(dt.datetime.now().year)
    db_path:str = None
    ctas_ctes:pd.DataFrame = field(init=False, repr=False)
    icaro:pd.DataFrame = field(init=False, repr=False)
    siif_rf602:pd.DataFrame = field(init=False, repr=False)
    siif_comprobantes:pd.DataFrame = field(init=False, repr=False)

    # --------------------------------------------------
    def __post_init__(self):
        if self.db_path == None:
            self.get_db_path()
        self.import_dfs()

    # --------------------------------------------------
    def import_dfs(self):
        self.import_ctas_ctes()
        self.import_icaro()
        self.import_siif_rf602()
        self.import_siif_comprobantes()

    # --------------------------------------------------
    def control_ejecucion_anual(self):
        icaro_por_estructura = self.icaro.copy()
        icaro_por_estructura = icaro_por_estructura >> \
            dplyr.filter_(f.tipo != 'PA6') >> \
            dplyr.mutate(estructura = f.actividad + '-' + f.partida) >> \
            dplyr.group_by(f.estructura, f.fuente) >> \
            dplyr.summarise(ejecucion_icaro = base.sum_(f.importe),
                            _groups = 'drop')
        siif_por_estructura = self.siif_rf602.copy()
        siif_por_estructura = siif_por_estructura >> \
            dplyr.select(f.estructura, f.fuente,
            ejecucion_siif = f.ordenado)
        ejecucion_anual = siif_por_estructura >> \
            dplyr.full_join(icaro_por_estructura) >> \
            tidyr.replace_na(0) >> \
            dplyr.mutate(
                diferencia = f.ejecucion_siif - f.ejecucion_icaro
            ) >> \
            dplyr.filter_(~dplyr.near(f.diferencia, 0))
        ejecucion_anual.sort_values(by=['estructura', 'fuente'], inplace= True)
        ejecucion_anual = pd.DataFrame(ejecucion_anual)
        return ejecucion_anual

    # --------------------------------------------------
    def control_comprobantes(self):
        siif = self.siif_comprobantes.copy()
        siif = siif >> \
            dplyr.select(
                siif_nro = f.nro_comprobante,
                siif_tipo = f.clase_reg,
                siif_fuente = f.fuente,
                siif_importe = f.importe,
                siif_fecha = f.fecha,
                siif_cta_cte = f.cta_cte,
                siif_cuit = f.cuit,
                siif_partida = f.partida
            )
        icaro = self.icaro.copy()
        icaro = icaro >> \
            dplyr.filter_(f.tipo != 'PA6') >> \
            dplyr.select(
                icaro_nro = f.nro_comprobante,
                icaro_tipo = f.tipo,
                icaro_fuente = f.fuente,
                icaro_importe = f.importe,
                icaro_fecha = f.fecha,
                icaro_cta_cte = f.cta_cte,
                icaro_cuit = f.cuit,
                icaro_partida = f.partida
            )
        comprobantes = siif >> \
            dplyr.full_join(icaro, by={'siif_nro':'icaro_nro'}, keep=True) >> \
            dplyr.mutate(
                dif_nro = f.siif_nro == f.icaro_nro,
                dif_tipo = f.siif_tipo == f.icaro_tipo,
                dif_fecha = f.siif_fecha == f.icaro_fecha,
                dif_partida = f.siif_partida == f.icaro_partida,
                dif_fuente = f.siif_fuente == f.icaro_fuente,
                dif_importe = f.siif_importe == f.icaro_importe,
                dif_cta_cte = f.siif_cta_cte == f.icaro_cta_cte,
                dif_cuit = f.siif_cuit == f.icaro_cuit
            ) >> \
            dplyr.select(
                f.siif_nro, f.icaro_nro, f.dif_nro,
                f.siif_tipo, f.icaro_tipo, f.dif_tipo,
                f.siif_fuente, f.icaro_fuente, f.dif_fuente,
                f.siif_importe, f.icaro_importe, f.dif_importe,
                f.siif_fecha, f.icaro_fecha, f.dif_fecha,
                f.siif_cta_cte, f.icaro_cta_cte, f.dif_cta_cte,
                f.siif_cuit, f.icaro_cuit, f.dif_cuit,
                f.siif_partida, f.icaro_partida, f.dif_partida
            ) >> \
            dplyr.filter_(
                ~f.dif_nro | ~f.dif_fecha | ~f.dif_partida |
                ~f.dif_fuente | ~f.dif_importe | ~f.dif_cta_cte |
                ~f.dif_cuit) >> \
            tidyr.replace_na(0)

        comprobantes = pd.DataFrame(comprobantes)
        comprobantes.sort_values(
            by=['dif_nro', 'dif_fuente', 'dif_importe', 
            'dif_cta_cte', 'dif_cuit', 'dif_partida', 
            'dif_fecha'],
            inplace=True)
        return comprobantes

    # --------------------------------------------------
    def get_db_path(self):
        self.db_path = os.path.dirname(
                        os.path.dirname(
                            os.path.dirname(
                                os.path.dirname(
                                    os.path.dirname(
                                        os.path.abspath(
                                            inspect.getfile(
                                                inspect.currentframe()))
                        )))))
        self.db_path += '/Python Output/SQLite Files'

    # --------------------------------------------------
    def import_ctas_ctes(self):
        df = CtasCtes().from_sql(self.db_path + '/sscc.sqlite') 
        self.ctas_ctes = df
        return self.ctas_ctes

    # --------------------------------------------------
    def import_icaro(self):
        df = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'carga')  
        df = df.loc[df['ejercicio'] == self.ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'icaro_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='icaro_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'icaro_cta_cte'], axis='columns', inplace=True)
        self.icaro = df
        return self.icaro

    # --------------------------------------------------
    def import_siif_rf602(self):
        df = PptoGtosFteRf602().from_sql(self.db_path + '/siif.sqlite')
        df = df.loc[df['ejercicio'] == self.ejercicio]
        df = df[df['partida'].isin(['421', '422'])]
        self.siif_rf602 = df
        return self.siif_rf602

    # --------------------------------------------------
    def import_siif_comprobantes(self):
        df = JoinComprobantesGtosGpoPart().from_sql_db(
            self.db_path + '/siif.sqlite')
        df = df.loc[df['ejercicio'] == self.ejercicio]
        df = df[df['partida'].isin(['421', '422'])]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'siif_gastos_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='siif_gastos_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'siif_gastos_cta_cte'], axis='columns', inplace=True)
        self.siif_comprobantes = df
        return self.siif_comprobantes

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Icaro vs SIIF budget execution",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-e', '--ejercicio', 
        metavar = "ejercicio",
        default= str(dt.datetime.now().year),
        type=str,
        help = "Ejercicio a filtrar")

    parser.add_argument(
        '-p', '--path', 
        metavar = "db_path",
        default= None,
        type=str,
        help = "Path to sqlite folder from where to import database")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    control = IcaroVsSIIF(
        ejercicio = args.ejercicio,
        db_path = args.path)
    df = control.control_comprobantes()
    print(df.head(5))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From src/invicoctrlpy/icaro/
    # python icaro_vs_siif.py -e 2021