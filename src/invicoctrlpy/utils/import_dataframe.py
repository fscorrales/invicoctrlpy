import datetime as dt
from dataclasses import dataclass, field

import pandas as pd
from datar import base, dplyr, f
from invicodatpy.icaro.migrate_icaro import MigrateIcaro
from invicodatpy.sgf.all import JoinResumenRendProvCuit, ResumenRendProv
from invicodatpy.sgv.all import (ResumenFacturado, ResumenRecaudado,
                                 SaldoBarrio, SaldoBarrioVariacion,
                                 SaldoMotivo,
                                 SaldoMotivoActualizacionSemestral,
                                 SaldoMotivoEntregaViviendas,
                                 SaldoRecuperosCobrarVariacion)
from invicodatpy.siif.all import (ComprobantesGtosRcg01Uejp,
                                  ComprobantesRecRci02, DeudaFlotanteRdeu012,
                                  DeudaFlotanteRdeu012b2C,
                                  JoinComprobantesGtosGpoPart,
                                  JoinPptoGtosFteDesc, MayorContableRcocc31,
                                  PptoGtosDescRf610, PptoGtosFteRf602,
                                  ResumenFdosRfondo07tp)
from invicodatpy.slave.migrate_slave import MigrateSlave
from invicodatpy.sscc.all import BancoINVICO, CtasCtes, SdoFinalBancoINVICO

from .hangling_path import HanglingPath


@dataclass
class ImportDataFrame(HanglingPath):
    db_path:str = field(init=False, repr=False)
    ctas_ctes:pd.DataFrame = field(init=False, repr=False)
    slave:pd.DataFrame = field(init=False, repr=False)
    icaro_carga:pd.DataFrame = field(init=False, repr=False)
    icaro_carga_neto_rdeu:pd.DataFrame = field(init=False, repr=False)
    siif_rdeu012:pd.DataFrame = field(init=False, repr=False)
    siif_rf602:pd.DataFrame = field(init=False, repr=False)
    siif_rfondo07tp:pd.DataFrame = field(init=False, repr=False)
    siif_rcg01_uejp:pd.DataFrame = field(init=False, repr=False)
    siif_comprobantes:pd.DataFrame = field(init=False, repr=False)
    siif_comprobantes_haberes:pd.DataFrame = field(init=False, repr=False)
    siif_comprobantes_haberes_neto_rdeu:pd.DataFrame = field(init=False, repr=False)
    siif_rcocc31:pd.DataFrame = field(init=False, repr=False)
    sgf_resumen_rend:pd.DataFrame = field(init=False, repr=False)
    sgf_resumen_rend_cuit:pd.DataFrame = field(init=False, repr=False)
    sgf_resumen_rend_honorarios:pd.DataFrame = field(init=False, repr=False)
    sscc_banco_invico:pd.DataFrame = field(init=False, repr=False)


    # --------------------------------------------------
    def import_ctas_ctes(self) -> pd.DataFrame:
        df = CtasCtes().from_sql(self.db_path + '/sscc.sqlite') 
        self.ctas_ctes = df
        return self.ctas_ctes

    # --------------------------------------------------
    def import_slave(self, ejercicio:str = None) -> pd.DataFrame:
        df = MigrateSlave().from_sql(
            self.db_path + '/slave.sqlite', 'honorarios_factureros')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)  
        self.slave = df
        return self.slave

    # --------------------------------------------------
    def import_icaro_desc_pres(self) -> pd.DataFrame:
        df_prog = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'programas')
        df_subprog = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'subprogramas')
        df_proy = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'proyectos')
        df_act = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'actividades')
        # Merge all
        df = df_act.merge(df_proy, how='left', on='proyecto', copy=False)
        df = df.merge(df_subprog, how='left', on=['subprograma'], copy=False)
        df = df.merge(df_prog, how='left', on=['programa'], copy=False)
        # Combine number with description
        df['desc_prog'] = df['actividad'].str[0:2] + ' - ' + df['desc_prog']
        df.desc_subprog.fillna(value='', inplace=True)
        df['desc_subprog'] = df['actividad'].str[3:5] + ' - ' + df['desc_subprog']
        df['desc_proy'] = df['actividad'].str[6:8] + ' - ' + df['desc_proy']
        df['desc_act'] = df['actividad'].str[9:11] + ' - ' + df['desc_act']

        df = df.loc[:, ['actividad','desc_prog','desc_subprog','desc_proy','desc_act']]
        return df

    # --------------------------------------------------
    def import_icaro_carga(self, ejercicio:str = None, 
                        neto_pa6:bool = False,
                        neto_reg:bool = False) -> pd.DataFrame:
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
        if neto_reg:
            df = df.loc[df['tipo'] != 'REG']
        self.icaro_carga = df
        return self.icaro_carga

    # --------------------------------------------------
    def import_icaro_carga_neto_rdeu(self, ejercicio:str) -> pd.DataFrame:
        #Neteamos los comprobantes de gastos no pagados (Deuda Flotante)
        icaro = self.import_icaro_carga(neto_pa6=True, neto_reg=True)
        # icaro = icaro.loc[~icaro['tipo'].isin(['REG', 'PA6'])]
        # icaro = icaro >> \
        #     dplyr.filter_(f.tipo != 'PA6')
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
            dplyr.bind_rows(icaro)
        icaro = self.import_icaro_carga()
        icaro = icaro.loc[icaro['tipo'].isin(['PA6'])]
        rdeu = rdeu >> \
            dplyr.bind_rows(icaro)
        self.icaro_carga_neto_rdeu = pd.DataFrame(rdeu)

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
            dplyr.filter_(f.ejercicio == ejercicio) >> \
            dplyr.left_join(
                dplyr.select(self.import_icaro_carga(neto_pa6=True, neto_reg=True),
                    f.nro_comprobante, f.actividad, f.partida, 
                    f.fondo_reparo, f.certificado, f.avance, 
                    f.origen, f.obra
                ),            
                by='nro_comprobante'
            ) >> \
            dplyr.mutate(
                importe = f.saldo,
                tipo = 'RDEU',
                id = f.nro_comprobante + 'C'
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
            dplyr.bind_rows(self.icaro_carga_neto_rdeu) 
        df = pd.DataFrame(rdeu)
        df = df.loc[df['ejercicio'] == ejercicio]
        self.icaro_carga_neto_rdeu = df
        return self.icaro_carga_neto_rdeu

    # --------------------------------------------------
    def import_icaro_retenciones(self) -> pd.DataFrame:
        df = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'retenciones')  
        # df = df.loc[df['tipo'] != 'REG']
        df.reset_index(drop=True, inplace=True)
        # if neto_pa6:
        #     df = df.loc[df['tipo'] != 'PA6']
        return df

    # --------------------------------------------------
    def import_icaro_obras(self) -> pd.DataFrame:
        df = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'obras')  
        return df

    # --------------------------------------------------
    def import_icaro_proveedores(self) -> pd.DataFrame:
        df = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'proveedores')  
        return df

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
    def import_siif_rdeu012b2_c(self, mes_hasta:str = None) -> pd.DataFrame:
        df = DeudaFlotanteRdeu012b2C().from_sql(self.db_path + '/siif.sqlite')
        if mes_hasta != None:
            df = df.loc[df['mes_hasta'] == mes_hasta]
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_rf602(self, ejercicio:str = None) -> pd.DataFrame:
        df = PptoGtosFteRf602().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        self.siif_rf602 = df
        return self.siif_rf602

    # --------------------------------------------------
    def import_siif_desc_pres(self, ejercicio_to:str = None) -> pd.DataFrame:
        df = PptoGtosDescRf610().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio_to != None:
            if isinstance(ejercicio_to, list):
                df = df.loc[df['ejercicio'].isin(ejercicio_to)]
            else:
                df = df.loc[df.ejercicio.astype(int) <= int(ejercicio_to)]
        df.sort_values(by=['ejercicio', 'estructura'], 
        inplace=True, ascending=[False, True])        
        # Programas ??nicos
        df_prog = df.loc[:, ['programa', 'desc_prog']]
        df_prog.drop_duplicates(subset=['programa'], inplace=True, keep='first')
        # Subprogramas ??nicos
        df_subprog = df.loc[:, ['programa', 'subprograma','desc_subprog']]
        df_subprog.drop_duplicates(
            subset=['programa', 'subprograma'], 
            inplace=True, keep='first')
        # Proyectos ??nicos
        df_proy = df.loc[:, [
            'programa', 'subprograma', 'proyecto', 'desc_proy']]
        df_proy.drop_duplicates(
            subset=['programa', 'subprograma', 'proyecto'], 
            inplace=True, keep='first')
        # Actividades ??nicos
        df_act = df.loc[:, [
            'estructura', 'programa', 'subprograma',
            'proyecto', 'actividad', 'desc_act',
            ]]
        df_act.drop_duplicates(subset=['estructura'], inplace=True, keep='first')
        # Merge all
        df = df_act.merge(df_prog, how='left', on='programa', copy=False)
        df = df.merge(
            df_subprog, how='left', on=['programa', 'subprograma'], 
            copy=False)
        df = df.merge(
            df_proy, how='left', on=['programa', 'subprograma', 'proyecto'], 
            copy=False)
        df['desc_prog'] = df.programa + ' - ' + df.desc_prog
        df['desc_subprog'] = df.subprograma + ' - ' + df.desc_subprog
        df['desc_proy'] = df.proyecto + ' - ' + df.desc_proy
        df['desc_act'] = df.actividad + ' - ' + df.desc_act
        df.drop(
            labels=['programa', 'subprograma', 'proyecto', 'actividad'], 
            axis=1, inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_ppto_gto_con_desc(self, ejercicio:str = None) -> pd.DataFrame:
        df = JoinPptoGtosFteDesc().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        return df

    # --------------------------------------------------
    def import_siif_rfondo07tp_pa6(self, ejercicio:str = None) -> pd.DataFrame:
        df = ResumenFdosRfondo07tp().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        df = df.loc[df['tipo_comprobante'] == 'ADELANTOS A CONTRATISTAS Y PROVEEDORES']
        self.siif_rfondo07tp = df
        return self.siif_rfondo07tp

    # --------------------------------------------------
    def import_siif_rcg01_uejp(self, ejercicio:str = None) -> pd.DataFrame:
        df = ComprobantesGtosRcg01Uejp().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'siif_gastos_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='siif_gastos_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'siif_gastos_cta_cte'], axis='columns', inplace=True)
        self.siif_rcg01_uejp = df
        return self.siif_rcg01_uejp

    def import_siif_comprobantes(self, ejercicio:list = None) -> pd.DataFrame:
        df = JoinComprobantesGtosGpoPart().from_sql(
            self.db_path + '/siif.sqlite')
        if ejercicio != None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'siif_gastos_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='siif_gastos_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'siif_gastos_cta_cte'], axis='columns', inplace=True)
        self.siif_comprobantes = df
        return self.siif_comprobantes

    def import_siif_comprobantes_fondos_perm(
        self, ejercicio:list = None
        ) -> pd.DataFrame:
        self.import_siif_comprobantes(ejercicio=ejercicio)
        df = self.siif_comprobantes.copy()
        if ejercicio != None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        df = df.loc[df['nro_fondo'].notnull()]
        df = df.loc[df['cta_cte'] == '130832-05']
        # nro_expte = df.loc[df['nro_fondo'].notnull()]
        # nro_expte = nro_expte.loc[nro_expte['cta_cte'] == '130832-05']
        # nro_expte = nro_expte['nro_expte'].unique()
        # df = df.loc[df['nro_expte'].isin(nro_expte)]
        return df

    def import_siif_comprobantes_haberes(
        self, ejercicio:str = None, neto_art:bool = False,
        neto_gcias_310:bool = False
        ) -> pd.DataFrame:
        self.import_siif_comprobantes(ejercicio=ejercicio)
        df = self.siif_comprobantes.copy()
        #df = df[df['grupo'] == '100']
        df = df[df['cta_cte'] == '130832-04']
        if neto_art:
            df = df.loc[~df['partida'].isin(['150', '151'])]
        if neto_gcias_310:
            self.import_siif_rcocc31(
                ejercicio=ejercicio, cta_contable='2122-1-2'
            )
            gcias_310 = self.siif_rcocc31.copy()
            gcias_310 = gcias_310[gcias_310['tipo_comprobante'] != 'APE']
            gcias_310 = gcias_310[gcias_310['auxiliar_1'].isin(['245', '310'])]
            gcias_310 = gcias_310 >> \
                dplyr.transmute(
                    ejercicio = f.ejercicio,
                    mes = f.mes,
                    fecha = f.fecha,
                    nro_comprobante = f.nro_entrada.str.zfill(5) + '/' + ejercicio[-2:],
                    importe = f.creditos * (-1),
                    grupo = '100',
                    partida = f.auxiliar_1,
                    nro_entrada = f.nro_entrada,
                    nro_origen = f.nro_entrada,
                    nro_expte = '90000000' + ejercicio,
                    glosa = dplyr.if_else(f.auxiliar_1 == '245',
                    'RET. GCIAS. 4TA CATEGOR??A', 'HABERES ERRONEOS COD 310'),
                    beneficiario = 'INSTITUTO DE VIVIENDA DE CORRIENTES',
                    nro_fondo = None,
                    fuente = '11',
                    cta_cte = '130832-04',
                    cuit = '30632351514',
                    clase_reg = 'CYO',
                    clase_mod = 'NOR',
                    clase_gto = 'REM',
                    es_comprometido = True,
                    es_verificado = True,
                    es_aprobado = True,
                    es_pagado = True
                )
            gcias_310 = pd.DataFrame(gcias_310)
            df = pd.concat([df, gcias_310])
        self.siif_comprobantes_haberes = pd.DataFrame(df)
        return self.siif_comprobantes_haberes

    # --------------------------------------------------
    def import_siif_comprobantes_haberes_neto_rdeu(
        self, ejercicio:str, neto_art:bool = False,
        neto_gcias_310:bool = False) -> pd.DataFrame:
        #Neteamos los comprobantes de gastos no pagados (Deuda Flotante)
        self.import_siif_comprobantes_haberes(
            ejercicio=ejercicio, neto_art=neto_art, neto_gcias_310=neto_gcias_310)
        comprobantes_haberes = self.siif_comprobantes_haberes.copy()
        rdeu = self.import_siif_rdeu012()
        rdeu = rdeu >> \
            dplyr.select(
                ~f.mes_hasta, ~f.fecha_aprobado, ~f.fecha_desde, 
                ~f.fecha_hasta, ~f.org_fin) >> \
            dplyr.distinct(f.nro_comprobante, f.mes, _keep_all=True) >> \
            dplyr.semi_join(comprobantes_haberes, by=f.nro_comprobante) >> \
            dplyr.distinct(f.nro_comprobante, f.mes, f.saldo, _keep_all=True) >> \
            dplyr.mutate(
                importe = f.saldo * (-1),
                clase_reg = 'CYO',
                clase_mod = 'NOR',
                clase_gto = 'RDEU',
                es_comprometido = True,
                es_verificado = True,
                es_aprobado = True,
                es_pagado = False
            )  >> \
            dplyr.select(~f.saldo)
        self.siif_comprobantes_haberes_neto_rdeu = pd.concat(
            [comprobantes_haberes, rdeu])

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
            dplyr.filter_(f.ejercicio == ejercicio) >> \
            dplyr.mutate(
                fecha = f.fecha_hasta, mes = f.mes_hasta
            ) >> \
            dplyr.select(
                ~f.mes_hasta, ~f.fecha_aprobado, ~f.fecha_desde, 
                ~f.fecha_hasta, ~f.org_fin) >> \
            dplyr.semi_join(comprobantes_haberes, by=f.nro_comprobante) >> \
            dplyr.distinct(f.nro_comprobante, f.mes, f.saldo, _keep_all=True) >> \
            dplyr.mutate(
                importe = f.saldo,
                clase_reg = 'CYO',
                clase_mod = 'NOR',
                clase_gto = 'RDEU',
                es_comprometido = True,
                es_verificado = True,
                es_aprobado = True,
                es_pagado = True
            ) >> \
            dplyr.select(~f.saldo)
        rdeu = pd.DataFrame(rdeu)
        rdeu = rdeu.loc[rdeu['ejercicio'] == ejercicio]
        self.siif_comprobantes_haberes_neto_rdeu = pd.concat(
            [self.siif_comprobantes_haberes_neto_rdeu, rdeu])
        return self.siif_comprobantes_haberes_neto_rdeu

    # --------------------------------------------------
    def import_siif_rci02(self, ejercicio:str = None) -> pd.DataFrame:
        df = ComprobantesRecRci02().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'siif_recursos_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='siif_recursos_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'siif_recursos_cta_cte'], axis='columns', inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_rcocc31(
        self, ejercicio:str = None, cta_contable:str = None) -> pd.DataFrame:
        df = MayorContableRcocc31().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        if cta_contable != None:
            df = df.loc[df['cta_contable'] == cta_contable]
        df.reset_index(drop=True, inplace=True)
        # map_to = self.ctas_ctes.loc[:,['map_to', 'siif_contabilidad_cta_cte']]
        # df = pd.merge(
        #     df, map_to, how='left',
        #     left_on='cta_cte', right_on='siif_contabilidad_cta_cte')
        # df['cta_cte'] = df['map_to']
        # df.drop(['map_to', 'siif_contabilidad_cta_cte'], axis='columns', inplace=True)
        self.siif_rcocc31 = df
        return self.siif_rcocc31

    # --------------------------------------------------
    def import_resumen_rend(self, ejercicio:str = None) -> pd.DataFrame:
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
        df_106 = df_106.loc[df_106['cta_cte'] == '106']
        df_106 = df_106.drop_duplicates(subset=[
            'mes', 'fecha', 'beneficiario', 
            'libramiento_sgf', 'importe_bruto'
        ])
        # df_106 = df.copy()
        # df_106 = df_106 >> \
        #     dplyr.filter_(f.cta_cte == '106') >> \
        #     dplyr.distinct(
        #         f.mes, f.fecha, f.beneficiario,
        #         f.libramiento_sgf, 
        #         _keep_all=True
        #     )
        df = df >> \
            dplyr.filter_(f.cta_cte != '106') >> \
            dplyr.bind_rows(df_106)
        self.sgf_resumen_rend = pd.DataFrame(df)
        return self.sgf_resumen_rend

    # --------------------------------------------------
    def import_resumen_rend_cuit(
        self, ejercicio:str = None, neto_cert_neg:bool=False) -> pd.DataFrame:
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
        df_106 = df_106.loc[df_106['cta_cte'] == '106']
        df_106 = df_106.drop_duplicates(subset=[
            'mes', 'fecha', 'beneficiario', 
            'libramiento_sgf', 'importe_bruto'
        ])
        # df_106 = df_106 >> \
        #     dplyr.filter_(f.cta_cte == '106') >> \
        #     dplyr.distinct(
        #         f.mes, f.fecha, f.beneficiario,
        #         f.libramiento_sgf, f.importe_bruto,
        #         _keep_all=True
        #     )
        df = df >> \
            dplyr.filter_(f.cta_cte != '106') >> \
            dplyr.bind_rows(df_106)
        if neto_cert_neg:
            self.import_banco_invico(ejercicio=ejercicio)
            banco_invico = self.sscc_banco_invico.copy()
            banco_invico = banco_invico >> \
                dplyr.filter_(f.cod_imputacion == '018') >> \
                dplyr.filter_(f.es_cheque == False) >> \
                dplyr.filter_(f.movimiento == 'DEPOSITO') >> \
                dplyr.mutate(
                    origen = 'BANCO',
                    cuit = '30632351514',
                    beneficiario = f.concepto,
                    destino = f.imputacion,
                    importe_bruto = f.importe * (-1),
                    importe_neto = f.importe_bruto
                ) >> \
                dplyr.select(
                    f.ejercicio, f.mes, f.fecha, f.cta_cte,
                    f.origen, f.cuit, f.beneficiario,
                    f.movimiento, f.destino, 
                    f.importe_bruto, f.importe_neto
                )
            df = df >> \
                dplyr.bind_rows(banco_invico, _copy=False)
        self.sgf_resumen_rend_cuit = pd.DataFrame(df)
        return self.sgf_resumen_rend_cuit

    # --------------------------------------------------
    def import_resumen_rend_honorarios(self, ejercicio:str = None, dep_emb:bool = True) -> pd.DataFrame:
        df = ResumenRendProv().from_sql(self.db_path + '/sgf.sqlite')  
        df = df.loc[df['origen'] != 'OBRAS']
        df = df.loc[df['cta_cte'].isin(['130832-05', '130832-07'])]
        df = df.loc[df['destino'].isin(['HONORARIOS - FUNCIONAMIENTO', 
        'COMISIONES - FUNCIONAMIENTO', 'HONORARIOS - EPAM'])]
        if ejercicio != None:
            df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'sgf_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='sgf_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'sgf_cta_cte'], axis='columns', inplace=True)
        if dep_emb:
            banco = self.import_banco_invico(ejercicio=self.ejercicio)
            banco = banco.loc[banco['cta_cte'] == '130832-05']
            banco = banco.loc[banco['cod_imputacion'] == '049']
            banco['importe_bruto'] = banco['importe'] * (-1)
            banco['importe_neto'] = banco['importe_bruto']
            banco['destino'] = 'EMBARGO POR ALIMENTOS'
            banco.rename(columns={
                "libramiento":"libramiento_sgf",
                }, inplace=True)
            banco = banco.loc[:, [
                'ejercicio', 'mes', 'fecha', 'beneficiario',
                'destino', 'cta_cte', 'libramiento_sgf', 'movimiento',
                'importe_bruto', 'importe_neto']]
            df = pd.concat([df, banco])
        self.sgf_resumen_rend_honorarios = pd.DataFrame(df)
        return self.sgf_resumen_rend_honorarios

    # --------------------------------------------------
    def import_banco_invico(self, ejercicio:str = None) -> pd.DataFrame:
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

    # --------------------------------------------------
    def import_sdo_final_banco_invico(self, ejercicio:str = None) -> pd.DataFrame:
        df = SdoFinalBancoINVICO().from_sql(self.db_path + '/sscc.sqlite')
        if ejercicio != None:  
            df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'sscc_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='sscc_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'sscc_cta_cte'], axis='columns', inplace=True)
        return df

    # --------------------------------------------------
    def import_saldo_barrio(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoBarrio().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio != None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_saldo_barrio_variacion(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoBarrioVariacion().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio != None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_saldo_recuperos_cobrar_variacion(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoRecuperosCobrarVariacion().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio != None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df
    
    # --------------------------------------------------
    def import_saldo_motivo(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoMotivo().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio != None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_saldo_motivo_entrega_viviendas(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoMotivoEntregaViviendas().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio != None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_saldo_motivo_actualizacion_semetral(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoMotivoActualizacionSemestral().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio != None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_resumen_facturado(self, ejercicio:str = None) -> pd.DataFrame:
        df = ResumenFacturado().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio != None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_resumen_recaudado(self, ejercicio:str = None) -> pd.DataFrame:
        df = ResumenRecaudado().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio != None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df
