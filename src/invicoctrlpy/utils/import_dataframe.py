import datetime as dt
from dataclasses import dataclass, field

from datar import dplyr, base, f

import pandas as pd
from invicodatpy.icaro.migrate_icaro import MigrateIcaro
from invicodatpy.siif.all import DeudaFlotanteRdeu012
from invicodatpy.sgf.all import JoinResumenRendProvCuit
from invicodatpy.sscc.all import CtasCtes

from .hangling_path import HanglingPath

@dataclass
class ImportDataFrame(HanglingPath):
    db_path:str = field(init=False, repr=False)
    ctas_ctes:pd.DataFrame = field(init=False, repr=False)
    icaro:pd.DataFrame = field(init=False, repr=False)
    icaro_neto_rdeu:pd.DataFrame = field(init=False, repr=False)
    siif_rdeu012:pd.DataFrame = field(init=False, repr=False)
    sgf_resumen_rend_cuit:pd.DataFrame = field(init=False, repr=False)
    sgf_resumen_rend_cuit:pd.DataFrame = field(init=False, repr=False)

    # --------------------------------------------------
    def import_ctas_ctes(self) -> pd.DataFrame:
        df = CtasCtes().from_sql(self.db_path + '/sscc.sqlite') 
        self.ctas_ctes = df
        return self.ctas_ctes

    # --------------------------------------------------
    def import_siif_rdeu012(self, ejercicio:str = None) -> pd.DataFrame:
        df = DeudaFlotanteRdeu012().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        # No estoy seguro del orden Desc o Asc
        df.sort_values(by=['fecha_hasta'], inplace=True, ascending=False)
        self.siif_rdeu012 = df
        return self.siif_rdeu012

    # --------------------------------------------------
    def import_icaro(self, ejercicio:str = None) -> pd.DataFrame:
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
        self.icaro = df
        return self.icaro

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
    def import_icaro_neto_rdeu(self, ejercicio:str) -> pd.DataFrame:
        #Neteamos los comprobantes de gastos no pagados (Deuda Flotante)
        icaro = self.import_icaro()
        icaro = icaro.loc[icaro['tipo'] != 'REG']
        icaro = icaro >> \
            dplyr.filter_(f.tipo != 'PA6')
        rdeu = self.import_siif_rdeu012()
        rdeu = rdeu >> \
            dplyr.select(f.nro_comprobante, f.saldo, mes = f.mes_hasta) >> \
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
        rdeu_lead = pd.DataFrame({'fecha_hasta':rdeu['fecha_hasta'].unique()})
        rdeu = rdeu_lead >> \
            dplyr.mutate(
                lead_fecha_hasta = dplyr.lead(f.fecha_hasta, default=base.NA)
            ) >> \
            dplyr.right_join(rdeu, by='fecha_hasta') >> \
            dplyr.filter_(~base.is_na(f.lead_fecha_hasta)) >> \
            dplyr.rename(
                fecha_borrar = 'fecha_hasta',
                fecha_hasta = 'lead_fecha_hasta'
            ) 

        rdeu['mes_hasta'] = (rdeu['fecha_hasta'].dt.month.astype(str).str.zfill(2) + 
                            '/' + rdeu['fecha_hasta'].dt.year.astype(str))

        rdeu = rdeu >> \
            dplyr.anti_join(self.siif_rdeu012)

        rdeu['ejercicio_ant'] = rdeu['fecha_borrar'].dt.year.astype(str)
        rdeu['ejercicio'] = rdeu['fecha_hasta'].dt.year.astype(str)

        # Incorporamos los comprobantes de gastos pagados 
        # en periodos posteriores (Deuda Flotante)
        rdeu = rdeu >> \
            dplyr.select(~f.fecha_borrar) >> \
            dplyr.filter_(f.ejercicio == ejercicio) >> \
            dplyr.semi_join(icaro, by='nro_comprobante') >> \
            dplyr.mutate(
                importe = f.saldo,
                tipo = 'RDEU'
            ) >> \
            dplyr.select(
                f.ejercicio, f.nro_comprobante, f.fuente,
                f.cuit, f.cta_cte, f.tipo, f.importe,
                fecha = f.fecha_hasta,
                mes = f.mes_hasta
            ) >> \
            dplyr.bind_rows(self.icaro_neto_rdeu) 
        df = pd.DataFrame(rdeu)
        df = df.loc[df['ejercicio'] == ejercicio]
        self.icaro_neto_rdeu = df
        return self.icaro_neto_rdeu