import datetime as dt
from dataclasses import dataclass, field

import pandas as pd
from datar import base, dplyr, f
from invicodatpy.icaro.migrate_icaro import MigrateIcaro
from invicodatpy.sgf.all import JoinResumenRendProvCuit, ResumenRendProv
from invicodatpy.siif.all import (ComprobantesRecRci02, DeudaFlotanteRdeu012,
                                  JoinComprobantesGtosGpoPart,
                                  PptoGtosFteRf602, ResumenFdosRfondo07tp)
from invicodatpy.sscc.all import BancoINVICO, CtasCtes

from .hangling_path import HanglingPath


@dataclass
class ImportDataFrame(HanglingPath):
    db_path:str = field(init=False, repr=False)
    ctas_ctes:pd.DataFrame = field(init=False, repr=False)
    icaro:pd.DataFrame = field(init=False, repr=False)
    icaro_neto_rdeu:pd.DataFrame = field(init=False, repr=False)
    siif_rdeu012:pd.DataFrame = field(init=False, repr=False)
    siif_rf602:pd.DataFrame = field(init=False, repr=False)
    siif_rfondo07tp:pd.DataFrame = field(init=False, repr=False)
    siif_comprobantes:pd.DataFrame = field(init=False, repr=False)
    siif_rci02:pd.DataFrame = field(init=False, repr=False)
    sgf_resumen_rend_cuit:pd.DataFrame = field(init=False, repr=False)
    sgf_resumen_rend:pd.DataFrame = field(init=False, repr=False)
    sscc_banco_invico:pd.DataFrame = field(init=False, repr=False)


    # --------------------------------------------------
    def import_ctas_ctes(self) -> pd.DataFrame:
        df = CtasCtes().from_sql(self.db_path + '/sscc.sqlite') 
        self.ctas_ctes = df
        return self.ctas_ctes

    # --------------------------------------------------
    def import_icaro(self, ejercicio:str = None, 
                        neto_pa6:bool = False) -> pd.DataFrame:
        df = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'carga')  
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        # df = df.loc[df['tipo'] != 'REG']
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'icaro_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='icaro_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'icaro_cta_cte'], axis='columns', inplace=True)
        if neto_pa6:
            df = df.loc[df['tipo'] != 'PA6']
        self.icaro = df
        return self.icaro

    # --------------------------------------------------
    def import_icaro_neto_rdeu(self, ejercicio:str) -> pd.DataFrame:
        #Neteamos los comprobantes de gastos no pagados (Deuda Flotante)
        icaro = self.import_icaro()
        icaro = icaro.loc[icaro['tipo'] != 'REG']
        icaro = icaro >> \
            dplyr.filter_(f.tipo != 'PA6')
        rdeu = self.import_siif_rdeu012()
        rdeu = rdeu >> \
            dplyr.select(f.nro_comprobante, f.saldo, f.mes) >> \
            dplyr.distinct(f.nro_comprobante, f.mes, _keep_all=True) >> \
            dplyr.inner_join(icaro) >> \
            dplyr.mutate(
                importe = f.saldo * (-1),
                tipo = 'RDEU'
            )  >> \
            dplyr.select(~f.saldo) >> \
            dplyr.bind_rows(self.icaro) 
        self.icaro_neto_rdeu = pd.DataFrame(rdeu)

        # Ajustamos la Deuda Flotante Pagada
        rdeu = self.import_siif_rdeu012()
        rdeu = rdeu.drop_duplicates(subset=['nro_comprobante'], keep='last')
        rdeu['fecha_hasta'] = (rdeu['fecha_hasta']
            + pd.tseries.offsets.DateOffset(months=1))
        rdeu['mes_hasta'] = rdeu['fecha_hasta'].dt.strftime('%m/%Y')
        rdeu['ejercicio'] = rdeu['mes_hasta'].str[-4:]

        # Incorporamos los comprobantes de gastos pagados 
        # en periodos posteriores (Deuda Flotante)
        rdeu = rdeu >> \
            dplyr.select(~f.fecha_borrar) >> \
            dplyr.filter_(f.ejercicio == ejercicio) >> \
            dplyr.left_join(
                dplyr.select(self.import_icaro(neto_pa6=True),
                    f.nro_comprobante, f.actividad, f.partida, 
                    f.fondo_reparo, f.certificado, f.avance, 
                    f.origen, f.obra
                ),            
                by='nro_comprobante'
            ) >> \
            dplyr.mutate(
                importe = f.saldo,
                tipo = 'RDEU',
                id = f.nro_comprobante
            ) >> \
            dplyr.filter_(~base.is_na(f.actividad)) >> \
            dplyr.select(
                f.ejercicio, f.nro_comprobante, f.fuente,
                f.cuit, f.cta_cte, f.tipo, f.importe, f.id,
                f.actividad, f.partida, f.fondo_reparo, 
                f.certificado, f.avance, f.origen, f.obra,
                fecha = f.fecha_hasta,
                mes = f.mes_hasta
            ) >> \
            dplyr.bind_rows(self.icaro_neto_rdeu) 
        df = pd.DataFrame(rdeu)
        df = df.loc[df['ejercicio'] == ejercicio]
        self.icaro_neto_rdeu = df
        return self.icaro_neto_rdeu

    # --------------------------------------------------
    def import_siif_rdeu012(self, ejercicio:str = None) -> pd.DataFrame:
        df = DeudaFlotanteRdeu012().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'siif_contabilidad_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='siif_contabilidad_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'siif_contabilidad_cta_cte'], axis='columns', inplace=True)
        # No estoy seguro del orden Desc o Asc
        df.sort_values(by=['fecha_hasta'], inplace=True, ascending=True)
        self.siif_rdeu012 = df
        return self.siif_rdeu012

    # --------------------------------------------------
    def import_siif_rf602(self, ejercicio:str = None):
        df = PptoGtosFteRf602().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == self.ejercicio]
        self.siif_rf602 = df
        return self.siif_rf602

    # --------------------------------------------------
    def import_siif_rfondo07tp_pa6(self, ejercicio:str = None):
        df = ResumenFdosRfondo07tp().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == self.ejercicio]
        df = df.loc[df['tipo_comprobante'] == 'ADELANTOS A CONTRATISTAS Y PROVEEDORES']
        self.siif_rfondo07tp = df
        return self.siif_rfondo07tp

    def import_siif_comprobantes(self, ejercicio:str = None):
        df = JoinComprobantesGtosGpoPart().from_sql(
            self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == self.ejercicio]
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
    def import_siif_rci02(self, ejercicio:str = None):
        df = ComprobantesRecRci02().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == self.ejercicio]
        self.siif_rci02 = df
        return self.siif_rci02

    # --------------------------------------------------
    def import_resumen_rend(self, ejercicio:str = None):
        df = ResumenRendProv().from_sql(self.db_path + '/sgf.sqlite')  
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'sgf_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='sgf_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'sgf_cta_cte'], axis='columns', inplace=True)
        #Filtramos los registros duplicados en la 106
        df_106 = df.copy()
        df_106 = df_106 >> \
            dplyr.filter_(f.cta_cte == '106') >> \
            dplyr.distinct(
                f.mes, f.fecha, f.beneficiario,
                f.libramiento_sgf, 
                _keep_all=True
            )
        df = df >> \
            dplyr.filter_(f.cta_cte != '106') >> \
            dplyr.bind_rows(df_106)
        self.sgf_resumen_rend = pd.DataFrame(df)
        return self.sgf_resumen_rend

    # --------------------------------------------------
    def import_resumen_rend_cuit(self, ejercicio:str = None) -> pd.DataFrame:
        df = JoinResumenRendProvCuit().from_sql(self.db_path + '/sgf.sqlite')  
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'sgf_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='sgf_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'sgf_cta_cte'], axis='columns', inplace=True)
        #Filtramos los registros duplicados en la 106
        df_106 = df.copy()
        df_106 = df_106 >> \
            dplyr.filter_(f.cta_cte == '106') >> \
            dplyr.distinct(
                f.mes, f.fecha, f.beneficiario,
                f.libramiento_sgf, 
                _keep_all=True
            )
        df = df >> \
            dplyr.filter_(f.cta_cte != '106') >> \
            dplyr.bind_rows(df_106)
        self.sgf_resumen_rend_cuit = pd.DataFrame(df)
        return self.sgf_resumen_rend_cuit

    # --------------------------------------------------
    def import_banco_invico(self, ejercicio:str = None):
        df = BancoINVICO().from_sql(self.db_path + '/sscc.sqlite')
        if ejercicio != None:  
            df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'sscc_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='sscc_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'sscc_cta_cte'], axis='columns', inplace=True)
        self.sscc_banco_invico = df
        return self.sscc_banco_invico