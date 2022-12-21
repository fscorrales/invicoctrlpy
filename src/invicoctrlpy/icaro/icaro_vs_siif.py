#!/usr/bin/env python3
"""
Author: Fernando Corrales <corrales_fernando@hotmail.com>
Purpose: Icaro vs SIIF budget execution
Data required:
    - Icaro
    - SIIF rf602
    - SIIF gto_rpa03g
    - SIIF rcg01_uejp
    - SIIF rfondo07tp
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
from invicodatpy.siif.resumen_fdos_rfondo07tp import ResumenFdosRfondo07tp
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
    siif_rfondo07tp:pd.DataFrame = field(init=False, repr=False)
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
        self.import_siif_rfondo07tp()
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
        ejecucion_anual.reset_index(drop=True, inplace=True)
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
                err_nro = f.siif_nro != f.icaro_nro,
                err_tipo = f.siif_tipo != f.icaro_tipo,
                err_fecha = f.siif_fecha != f.icaro_fecha,
                err_partida = f.siif_partida != f.icaro_partida,
                err_fuente = f.siif_fuente != f.icaro_fuente,
                err_importe = ~dplyr.near(f.siif_importe - f.icaro_importe, 0),
                err_cta_cte = f.siif_cta_cte != f.icaro_cta_cte,
                err_cuit = f.siif_cuit != f.icaro_cuit
            ) >>\
            dplyr.select(
                f.siif_nro, f.icaro_nro, f.err_nro,
                f.siif_tipo, f.icaro_tipo, f.err_tipo,
                f.siif_fuente, f.icaro_fuente, f.err_fuente,
                f.siif_importe, f.icaro_importe, f.err_importe,
                f.siif_fecha, f.icaro_fecha, f.err_fecha,
                f.siif_cta_cte, f.icaro_cta_cte, f.err_cta_cte,
                f.siif_cuit, f.icaro_cuit, f.err_cuit,
                f.siif_partida, f.icaro_partida, f.err_partida
            ) >> \
            dplyr.filter_(
                f.err_nro | f.err_fecha | f.err_partida |
                f.err_fuente | f.err_importe | f.err_cta_cte |
                f.err_cuit) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            )
            # dplyr.mutate(
            #     err_nro = dplyr.if_else(f.err_nro > 0, "\u2713", "\u2718")
            # )

        comprobantes = pd.DataFrame(comprobantes)
        comprobantes.sort_values(
            by=['err_nro', 'err_fuente', 'err_importe', 
            'err_cta_cte', 'err_cuit', 'err_partida', 
            'err_fecha'], ascending=False,
            inplace=True)
        comprobantes.reset_index(drop=True, inplace=True)
        return comprobantes

    # --------------------------------------------------
    def control_pa6(self):
        siif_fdos = self.siif_rfondo07tp.copy()
        siif_fdos = siif_fdos >> \
            dplyr.select(
                siif_nro_fondo = f.nro_fondo,
                siif_fecha_pa6 = f.fecha,
                siif_importe_pa6 = f.ingresos,
                siif_saldo_pa6 = f.saldo
            )
        
        siif_gtos = self.siif_comprobantes.copy()
        siif_gtos = siif_gtos.loc[siif_gtos['clase_reg'] == 'REG']
        siif_gtos = siif_gtos >> \
            dplyr.select(
                siif_nro_fondo = f.nro_fondo,
                siif_cta_cte = f.cta_cte, 
                siif_cuit = f.cuit,
                siif_tipo = f.clase_reg, 
                siif_fuente =f.fuente, 
                siif_nro_reg = f.nro_comprobante, 
                siif_importe_reg = f.importe, 
                siif_fecha_reg = f.fecha
            )
        
        icaro = self.icaro
        icaro = icaro >> \
            dplyr.select(
                icaro_fecha = f.fecha,
                icaro_nro = f.nro_comprobante, 
                icaro_tipo = f.tipo, 
                icaro_importe = f.importe,
                icaro_cuit = f.cuit,
                icaro_cta_cte = f.cta_cte,
                icaro_fuente = f.fuente
            )
        
        icaro_pa6 =  icaro >> \
            dplyr.filter_(f.icaro_tipo == 'PA6') >> \
            dplyr.select(
                icaro_nro_fondo = f.icaro_nro,
                icaro_fecha_pa6 = f.icaro_fecha,
                icaro_importe_pa6 = f.icaro_importe
            )

        icaro_reg =  icaro >> \
            dplyr.filter_(f.icaro_tipo != 'PA6') >> \
            dplyr.select(
                f.icaro_cta_cte, 
                f.icaro_cuit, 
                f.icaro_fuente,
                f.icaro_tipo, 
                icaro_nro_reg = f.icaro_nro,
                icaro_fecha_reg = f.icaro_fecha,
                icaro_importe_reg = f.icaro_importe,
            )
        
        control_pa6 = siif_fdos >> \
            dplyr.left_join(siif_gtos, by='siif_nro_fondo', keep=False) >>\
            dplyr.mutate(
                siif_nro_fondo = f.siif_nro_fondo.str.zfill(5) + '/' + self.ejercicio[-2:]
            ) >> \
            dplyr.full_join(
                icaro_pa6, by={'siif_nro_fondo':'icaro_nro_fondo'}, 
                keep=True
            ) >> \
            dplyr.left_join(
                icaro_reg, by={'siif_nro_reg':'icaro_nro_reg'}, 
                keep=False
            )  >> \
            dplyr.mutate(
                err_nro_fondo = f.siif_nro_fondo != f.icaro_nro_fondo,
                err_fecha_pa6 = f.siif_fecha_pa6 != f.icaro_fecha_pa6,
                err_importe_pa6 = ~dplyr.near(f.siif_importe_pa6 - f.icaro_importe_pa6, 0),
                err_fecha_reg = f.siif_fecha_reg != f.icaro_fecha_reg,
                err_importe_reg = ~dplyr.near(f.siif_importe_reg - f.icaro_importe_reg, 0),
                err_tipo = f.siif_tipo != f.icaro_tipo,
                err_fuente = f.siif_fuente != f.icaro_fuente,
                err_cta_cte = f.siif_cta_cte != f.icaro_cta_cte,
                err_cuit = f.siif_cuit != f.icaro_cuit
            ) >>\
            dplyr.select(
                f.siif_nro_fondo, f.icaro_nro_fondo, f.err_nro_fondo,
                f.siif_fecha_pa6, f.icaro_fecha_pa6, f.err_fecha_pa6,
                f.siif_importe_pa6, f.icaro_importe_pa6, f.err_importe_pa6,
                f.siif_fecha_reg, f.icaro_fecha_reg, f.err_fecha_reg,
                f.siif_importe_reg, f.icaro_importe_reg, f.err_importe_reg,
                f.siif_tipo, f.icaro_tipo, f.err_tipo,
                f.siif_fuente, f.icaro_fuente, f.err_fuente,
                f.siif_cta_cte, f.icaro_cta_cte, f.err_cta_cte,
                f.siif_cuit, f.icaro_cuit, f.err_cuit
            ) >> \
            dplyr.filter_(
                f.err_nro_fondo | f.err_fecha_pa6 | f.err_importe_pa6 |
                f.err_fecha_reg | f.err_importe_reg |
                f.err_fuente | f.err_tipo | f.err_cta_cte |
                f.err_cuit) >> \
            dplyr.mutate(
                dplyr.across(dplyr.where(base.is_numeric), tidyr.replace_na, 0)
            )

        control_pa6 = pd.DataFrame(control_pa6)
        control_pa6.sort_values(
            by=['err_nro_fondo',
            'err_importe_pa6', 'err_importe_reg', 
            'err_fuente', 'err_cta_cte', 'err_cuit', 
            'err_tipo', 'err_fecha_pa6', 'err_fecha_reg'], 
            ascending=False, inplace=True)
        control_pa6.reset_index(drop=True, inplace=True)
        return control_pa6

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
    def import_siif_rfondo07tp(self):
        df = ResumenFdosRfondo07tp().from_sql(self.db_path + '/siif.sqlite')
        df = df.loc[df['ejercicio'] == self.ejercicio]
        df = df.loc[df['tipo_comprobante'] == 'ADELANTOS A CONTRATISTAS Y PROVEEDORES']
        self.siif_rfondo07tp = df
        return self.siif_rfondo07tp

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
    df = control.control_pa6()
    print(df.head(5))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From src/invicoctrlpy/icaro/
    # python icaro_vs_siif.py -e 2021