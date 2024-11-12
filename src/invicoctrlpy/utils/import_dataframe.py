from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from invicodatpy.icaro.migrate_icaro import MigrateIcaro
from invicodatpy.sgf.all import JoinResumenRendProvCuit, ResumenRendProv
from invicodatpy.sgv.all import (ResumenFacturado, ResumenRecaudado,
                                 SaldoBarrio, SaldoBarrioVariacion,
                                 SaldoMotivo, BarriosNuevos,
                                 SaldoMotivoPorBarrio,
                                 SaldoRecuperosCobrarVariacion)
from invicodatpy.siif.all import (ComprobantesGtosRcg01Uejp,
                                  ComprobantesRecRci02, DeudaFlotanteRdeu012,
                                  DeudaFlotanteRdeu012b2C,
                                  JoinComprobantesGtosGpoPart,
                                  JoinPptoGtosFteDesc, MayorContableRcocc31,
                                  PptoGtosDescRf610, PptoGtosFteRf602,
                                  PptoRecRi102, ResumenFdosRfondo07tp, FormGtoRfpP605b)
from invicodatpy.slave.migrate_slave import MigrateSlave
from invicodatpy.sscc.all import BancoINVICO, CtasCtes, SdoFinalBancoINVICO
from invicodatpy.sgo.all import ListadoObras

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
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
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
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        # if ejercicio != None:
        #     df = df.loc[df['ejercicio'] == ejercicio]
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
    def import_icaro_obras(self) -> pd.DataFrame:
        df = MigrateIcaro().from_sql(self.db_path + '/icaro.sqlite', 'obras')  
        return df

    # --------------------------------------------------
    def import_icaro_carga_neto_rdeu(self, ejercicio:str) -> pd.DataFrame:
        #Neteamos los comprobantes de gastos no pagados (Deuda Flotante)
        icaro = self.import_icaro_carga(neto_pa6=True, neto_reg=True)
        # icaro = icaro.loc[~icaro['tipo'].isin(['REG', 'PA6'])]
        # icaro = icaro >> \
        #     dplyr.filter_(f.tipo != 'PA6')
        rdeu = self.import_siif_rdeu012()
        rdeu = rdeu.loc[:, ['nro_comprobante', 'saldo', 'mes']]
        rdeu = rdeu.drop_duplicates(subset=['nro_comprobante', 'mes'])
        rdeu = pd.merge(
            rdeu, icaro, how='inner', copy=False
        )
        rdeu['importe'] = rdeu.saldo * (-1)
        rdeu['tipo'] = 'RDEU'
        rdeu = rdeu.drop(columns=['saldo'])
        rdeu = pd.concat([rdeu, icaro], copy=False)
        # rdeu = rdeu >> \
        #     dplyr.select(f.nro_comprobante, f.saldo, f.mes) >> \
        #     dplyr.distinct(f.nro_comprobante, f.mes, _keep_all=True) >> \
        #     dplyr.inner_join(icaro) >> \
        #     dplyr.mutate(
        #         importe = f.saldo * (-1),
        #         tipo = 'RDEU'
        #     )  >> \
        #     dplyr.select(~f.saldo) >> \
        #     dplyr.bind_rows(icaro)
        icaro = self.import_icaro_carga()
        icaro = icaro.loc[icaro['tipo'].isin(['PA6'])]
        rdeu = pd.concat([rdeu, icaro], copy=False)
        icaro_carga_neto_rdeu = rdeu
        # rdeu = rdeu >> \
        #     dplyr.bind_rows(icaro)
        # self.icaro_carga_neto_rdeu = pd.DataFrame(rdeu)

        # Ajustamos la Deuda Flotante Pagada
        rdeu = self.import_siif_rdeu012()
        rdeu = rdeu.drop_duplicates(subset=['nro_comprobante'], keep='last')
        rdeu['fecha_hasta'] = (rdeu['fecha_hasta']
            + pd.tseries.offsets.DateOffset(months=1))
        rdeu['mes_hasta'] = rdeu['fecha_hasta'].dt.strftime('%m/%Y')
        rdeu['ejercicio'] = rdeu['mes_hasta'].str[-4:]

        # Incorporamos los comprobantes de gastos pagados 
        # en periodos posteriores (Deuda Flotante)
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                rdeu = rdeu.loc[rdeu['ejercicio'].isin(ejercicio)]
            else:
                rdeu = rdeu.loc[rdeu['ejercicio'].isin([ejercicio])]
        icaro = self.import_icaro_carga(neto_pa6=True, neto_reg=True)
        icaro = icaro.loc[:, [
            'nro_comprobante', 'actividad', 'partida', 
            'fondo_reparo', 'certificado', 'avance', 
            'origen', 'obra'
        ]]
        rdeu = pd.merge(rdeu, icaro, on='nro_comprobante', copy=False)
        rdeu['importe'] = rdeu.saldo
        rdeu['tipo'] = 'RDEU'
        rdeu['id'] =  rdeu['nro_comprobante'] + 'C'
        rdeu = rdeu.loc[~rdeu['actividad'].isna()]
        rdeu = rdeu.drop(columns=['fecha', 'mes'])
        rdeu = rdeu.rename(columns={
            'fecha_hasta':'fecha',
            'mes_hasta':'mes'
        })
        rdeu = rdeu.loc[:, [
            'ejercicio', 'nro_comprobante', 'fuente',
            'cuit', 'cta_cte', 'tipo', 'importe', 'id',
            'actividad', 'partida', 'fondo_reparo', 
            'certificado', 'avance', 'origen', 'obra',
            'fecha', 'mes'
        ]]
        df = pd.concat([rdeu, icaro_carga_neto_rdeu], copy=False)

        # rdeu = rdeu >> \
        #     dplyr.filter_(f.ejercicio == ejercicio) >> \
        #     dplyr.left_join(
        #         dplyr.select(self.import_icaro_carga(neto_pa6=True, neto_reg=True),
        #             f.nro_comprobante, f.actividad, f.partida, 
        #             f.fondo_reparo, f.certificado, f.avance, 
        #             f.origen, f.obra
        #         ),            
        #         by='nro_comprobante'
        #     ) >> \
        #     dplyr.mutate(
        #         importe = f.saldo,
        #         tipo = 'RDEU',
        #         id = f.nro_comprobante + 'C'
        #     ) >> \
        #     dplyr.filter_(~base.is_na(f.actividad)) >> \
        #     dplyr.select(
        #         f.ejercicio, f.nro_comprobante, f.fuente,
        #         f.cuit, f.cta_cte, f.tipo, f.importe, f.id,
        #         f.actividad, f.partida, f.fondo_reparo, 
        #         f.certificado, f.avance, f.origen, f.obra,
        #         fecha = f.fecha_hasta,
        #         mes = f.mes_hasta
        #     ) >> \
        #     dplyr.bind_rows(icaro_carga_neto_rdeu) 
        # df = pd.DataFrame(rdeu)
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        # self.icaro_carga_neto_rdeu = df
        return df

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
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        df.reset_index(drop=True, inplace=True)
        map_to = self.import_ctas_ctes().loc[:,['map_to', 'siif_contabilidad_cta_cte']]
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
        if mes_hasta is not None:
            df = df.loc[df['mes_hasta'] == mes_hasta]
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_rf602(self, ejercicio:str = None) -> pd.DataFrame:
        df = PptoGtosFteRf602().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        self.siif_rf602 = df
        return self.siif_rf602

    # --------------------------------------------------
    def import_siif_rfp_p605b(self, ejercicio:str = None) -> pd.DataFrame:
        df = FormGtoRfpP605b().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        # if ejercicio != None:
        #     df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_desc_pres(self, ejercicio_to:str = None) -> pd.DataFrame:
        df = PptoGtosDescRf610().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio_to is not None:
            if isinstance(ejercicio_to, list):
                df = df.loc[df['ejercicio'].isin(ejercicio_to)]
            else:
                df = df.loc[df.ejercicio.astype(int) <= int(ejercicio_to)]
        df.sort_values(by=['ejercicio', 'estructura'], 
        inplace=True, ascending=[False, True])        
        # Programas únicos
        df_prog = df.loc[:, ['programa', 'desc_prog']]
        df_prog.drop_duplicates(subset=['programa'], inplace=True, keep='first')
        # Subprogramas únicos
        df_subprog = df.loc[:, ['programa', 'subprograma','desc_subprog']]
        df_subprog.drop_duplicates(
            subset=['programa', 'subprograma'], 
            inplace=True, keep='first')
        # Proyectos únicos
        df_proy = df.loc[:, [
            'programa', 'subprograma', 'proyecto', 'desc_proy']]
        df_proy.drop_duplicates(
            subset=['programa', 'subprograma', 'proyecto'], 
            inplace=True, keep='first')
        # Actividades únicos
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
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        return df

    # --------------------------------------------------
    def import_siif_rfondo07tp_pa6(self, ejercicio:str = None) -> pd.DataFrame:
        df = ResumenFdosRfondo07tp().from_sql(self.db_path + '/siif.sqlite')
        # if ejercicio != None:
        #     df = df.loc[df['ejercicio'] == ejercicio]
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        df = df.loc[df['tipo_comprobante'] == 'ADELANTOS A CONTRATISTAS Y PROVEEDORES']
        self.siif_rfondo07tp = df
        return self.siif_rfondo07tp

    # --------------------------------------------------
    def import_siif_rcg01_uejp(self, ejercicio:str = None) -> pd.DataFrame:
        df = ComprobantesGtosRcg01Uejp().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio is not None:
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
        self.siif_rcg01_uejp = df
        return self.siif_rcg01_uejp

    def import_siif_comprobantes(self, ejercicio:list = None) -> pd.DataFrame:
        df = JoinComprobantesGtosGpoPart().from_sql(
            self.db_path + '/siif.sqlite')
        if ejercicio is not None:
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
        if ejercicio is not None:
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
        df = self.import_siif_comprobantes(ejercicio=ejercicio).copy()
        #df = df[df['grupo'] == '100']
        df = df[df['cta_cte'] == '130832-04']
        if neto_art:
            df = df.loc[~df['partida'].isin(['150', '151'])]
        if neto_gcias_310:
            gcias_310 = self.import_siif_rcocc31(
                ejercicio=ejercicio, cta_contable='2122-1-2'
            ).copy()
            gcias_310 = gcias_310[gcias_310['tipo_comprobante'] != 'APE']
            gcias_310 = gcias_310[gcias_310['auxiliar_1'].isin(['245', '310'])]
            gcias_310['nro_comprobante'] = gcias_310['nro_entrada'].str.zfill(5) + '/' + gcias_310['ejercicio'].str[-2:] + 'A'
            gcias_310['importe'] = gcias_310['creditos'] * (-1)
            gcias_310['grupo'] = '100'
            gcias_310['partida'] = gcias_310['auxiliar_1']
            gcias_310['nro_origen'] = gcias_310['nro_entrada']
            gcias_310['nro_expte'] = '90000000' + gcias_310['ejercicio']
            gcias_310['glosa'] = np.where(gcias_310['auxiliar_1'] == '245', 
                                'RET. GCIAS. 4TA CATEGORÍA', 'HABERES ERRONEOS COD 310')
            gcias_310['beneficiario'] = 'INSTITUTO DE VIVIENDA DE CORRIENTES'
            gcias_310['nro_fondo'] = None
            gcias_310['fuente'] = '11'
            gcias_310['cta_cte'] = '130832-04'
            gcias_310['cuit'] = '30632351514'
            gcias_310['clase_reg'] = 'CYO'
            gcias_310['clase_mod'] = 'NOR'
            gcias_310['clase_gto'] = 'REM'
            gcias_310['es_comprometido'] = True
            gcias_310['es_verificado'] = True
            gcias_310['es_aprobado'] = True
            gcias_310['es_pagado'] = True
            gcias_310 = gcias_310.loc[
                :, [
                    'ejercicio', 'mes', 'fecha', 'nro_comprobante',
                    'importe', 'grupo', 'partida', 'nro_entrada', 
                    'nro_origen', 'nro_expte', 'glosa', 'beneficiario', 
                    'nro_fondo', 'fuente', 'cta_cte', 'cuit',
                    'clase_reg', 'clase_mod', 'clase_gto', 'es_comprometido',
                    'es_verificado', 'es_aprobado', 'es_pagado',
                ]
            ]
            df = pd.concat([df, gcias_310])
        self.siif_comprobantes_haberes = pd.DataFrame(df)
        return self.siif_comprobantes_haberes

    # --------------------------------------------------
    def import_siif_comprobantes_haberes_neto_rdeu(
        self, ejercicio:str, neto_art:bool = False,
        neto_gcias_310:bool = False) -> pd.DataFrame:
        #Neteamos los comprobantes de gastos no pagados (Deuda Flotante)
        comprobantes_haberes =  self.import_siif_comprobantes_haberes(
            ejercicio=ejercicio, neto_art=neto_art, neto_gcias_310=neto_gcias_310
        ).copy()
        rdeu = self.import_siif_rdeu012()
        rdeu = rdeu.drop(columns=[
            'mes_hasta', 'fecha_aprobado', 'fecha_desde', 'fecha_hasta', 'org_fin'
        ])
        rdeu = rdeu.drop_duplicates(subset=['nro_comprobante', 'mes'])
        semi_table = pd.merge(
            rdeu, comprobantes_haberes, how='inner', copy=False, on='nro_comprobante'
        )
        in_both = rdeu['nro_comprobante'].isin(semi_table['nro_comprobante'])
        rdeu = rdeu[in_both] 
        rdeu = rdeu.drop_duplicates(subset=['nro_comprobante', 'mes', 'saldo'])
        rdeu['importe'] = rdeu['saldo'] * (-1)
        rdeu['clase_reg'] = 'CYO'
        rdeu['clase_nor'] = 'NOR'
        rdeu['clase_gto'] = 'RDEU'
        rdeu['es_comprometido'] = True
        rdeu['es_verificado'] = True
        rdeu['es_aprobado'] = True
        rdeu['es_pagado'] = True
        rdeu = rdeu.drop(columns=['saldo'])
        # rdeu = rdeu >> \
        #     dplyr.select(
        #         ~f.mes_hasta, ~f.fecha_aprobado, ~f.fecha_desde, 
        #         ~f.fecha_hasta, ~f.org_fin) >> \
        #     dplyr.distinct(f.nro_comprobante, f.mes, _keep_all=True) >> \
        #     dplyr.semi_join(comprobantes_haberes, by=f.nro_comprobante) >> \
        #     dplyr.distinct(f.nro_comprobante, f.mes, f.saldo, _keep_all=True) >> \
        #     dplyr.mutate(
        #         importe = f.saldo * (-1),
        #         clase_reg = 'CYO',
        #         clase_mod = 'NOR',
        #         clase_gto = 'RDEU',
        #         es_comprometido = True,
        #         es_verificado = True,
        #         es_aprobado = True,
        #         es_pagado = False
        #     )  >> \
        #     dplyr.select(~f.saldo)
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
        if isinstance(ejercicio, list):
            rdeu = rdeu.loc[rdeu['ejercicio'].isin(ejercicio)]
        else:
            rdeu = rdeu.loc[rdeu['ejercicio'].isin([ejercicio])]
        rdeu['fecha'] = rdeu['fecha_hasta']
        rdeu['mes'] = rdeu['mes_hasta']
        rdeu = rdeu.drop(columns=[
            'mes_hasta', 'fecha_aprobado', 'fecha_desde', 'fecha_hasta', 'org_fin'
        ])
        semi_table = pd.merge(
            rdeu, comprobantes_haberes, how='inner', copy=False, on='nro_comprobante'
        )
        in_both = rdeu['nro_comprobante'].isin(semi_table['nro_comprobante'])
        rdeu = rdeu[in_both]
        rdeu = rdeu.drop_duplicates(subset=['nro_comprobante', 'mes', 'saldo'])
        rdeu['importe'] = rdeu['saldo']
        rdeu['clase_reg'] = 'CYO'
        rdeu['clase_nor'] = 'NOR'
        rdeu['clase_gto'] = 'RDEU'
        rdeu['es_comprometido'] = True
        rdeu['es_verificado'] = True
        rdeu['es_aprobado'] = True
        rdeu['es_pagado'] = True
        rdeu = rdeu.drop(columns=['saldo'])

        # rdeu = rdeu >> \
        #     dplyr.filter_(f.ejercicio == ejercicio) >> \
        #     dplyr.mutate(
        #         fecha = f.fecha_hasta, mes = f.mes_hasta
        #     ) >> \
        #     dplyr.select(
        #         ~f.mes_hasta, ~f.fecha_aprobado, ~f.fecha_desde, 
        #         ~f.fecha_hasta, ~f.org_fin) >> \
        #     dplyr.semi_join(comprobantes_haberes, by=f.nro_comprobante) >> \
        #     dplyr.distinct(f.nro_comprobante, f.mes, f.saldo, _keep_all=True) >> \
        #     dplyr.mutate(
        #         importe = f.saldo,
        #         clase_reg = 'CYO',
        #         clase_mod = 'NOR',
        #         clase_gto = 'RDEU',
        #         es_comprometido = True,
        #         es_verificado = True,
        #         es_aprobado = True,
        #         es_pagado = True
        #     ) >> \
        #     dplyr.select(~f.saldo)
        # rdeu = pd.DataFrame(rdeu)
        # rdeu = rdeu.loc[rdeu['ejercicio'] == ejercicio]
        if isinstance(ejercicio, list):
            rdeu = rdeu.loc[rdeu['ejercicio'].isin(ejercicio)]
        else:
            rdeu = rdeu.loc[rdeu['ejercicio'].isin([ejercicio])]
        self.siif_comprobantes_haberes_neto_rdeu = pd.concat(
            [self.siif_comprobantes_haberes_neto_rdeu, rdeu])
        return self.siif_comprobantes_haberes_neto_rdeu

    # --------------------------------------------------
    def import_siif_rci02(self, ejercicio:str = None) -> pd.DataFrame:
        df = ComprobantesRecRci02().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        # if ejercicio != None:
        #     df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'siif_recursos_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='siif_recursos_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'siif_recursos_cta_cte'], axis='columns', inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_ri102(self, ejercicio:str = None) -> pd.DataFrame:
        df = PptoRecRi102().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        # if ejercicio != None:
        #     df = df.loc[df['ejercicio'] == ejercicio]
        df.reset_index(drop=True, inplace=True)
        return df

    # --------------------------------------------------
    def import_siif_rcocc31(
        self, ejercicio:str = None, cta_contable:str = None) -> pd.DataFrame:
        df = MayorContableRcocc31().from_sql(self.db_path + '/siif.sqlite')
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        if cta_contable is not None:
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
        if ejercicio is not None:
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
        df = pd.concat([df[df['cta_cte'] != '106'], df_106])
        # df = df >> \
            # dplyr.filter_(f.cta_cte != '106') >> \
            # dplyr.bind_rows(df_106)
        self.sgf_resumen_rend = pd.DataFrame(df)
        return self.sgf_resumen_rend

    # --------------------------------------------------
    def import_resumen_rend_cuit(
        self, ejercicio:str = None, neto_cert_neg:bool=False) -> pd.DataFrame:
        df = JoinResumenRendProvCuit().from_sql(self.db_path + '/sgf.sqlite')  
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
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
        df = pd.concat([df[df['cta_cte'] != '106'], df_106], ignore_index=True)
        # df = df >> \
        #     dplyr.filter_(f.cta_cte != '106') >> \
        #     dplyr.bind_rows(df_106)
        #Filtramos los registros duplicados en la 106
        df_2210178150 = df.copy()
        df_2210178150 = df_2210178150.loc[df_2210178150['cta_cte'] == '2210178150']
        df_2210178150 = df_2210178150.drop_duplicates(subset=[
            'mes', 'fecha', 'beneficiario', 
            'libramiento_sgf', 'importe_bruto'
        ])
        df = df[df['cta_cte'] != '2210178150']
        df = pd.concat(
            [df[df['cta_cte'] != '2210178150'], df_2210178150], 
            ignore_index=True
        )
        # df = df >> \
        #     dplyr.filter_(f.cta_cte != '2210178150') >> \
        #     dplyr.bind_rows(df_2210178150)
        if neto_cert_neg:
            self.import_banco_invico(ejercicio=ejercicio)
            banco_invico = self.sscc_banco_invico.copy()
            banco_invico = banco_invico.loc[(banco_invico['cod_imputacion'] == '018') & 
                                            (banco_invico['es_cheque'] == False) & 
                                            (banco_invico['movimiento'] == 'DEPOSITO')]
            banco_invico['origen'] = 'BANCO'
            banco_invico['cuit'] = '30632351514'
            banco_invico['beneficiario'] = banco_invico['concepto']
            banco_invico['destino'] = banco_invico['imputacion']
            banco_invico['importe_bruto'] = banco_invico['importe'] * (-1)
            banco_invico['importe_neto'] = banco_invico['importe_bruto']
            banco_invico = banco_invico.loc[:, [
                'ejercicio', 'mes', 'fecha', 'cta_cte',
                'origen', 'cuit', 'beneficiario',
                'movimiento', 'destino', 
                'importe_bruto', 'importe_neto'
            ]]
            # banco_invico = banco_invico >> \
            #     dplyr.filter_(f.cod_imputacion == '018') >> \
            #     dplyr.filter_(f.es_cheque == False) >> \
            #     dplyr.filter_(f.movimiento == 'DEPOSITO') >> \
            #     dplyr.mutate(
            #         origen = 'BANCO',
            #         cuit = '30632351514',
            #         beneficiario = f.concepto,
            #         destino = f.imputacion,
            #         importe_bruto = f.importe * (-1),
            #         importe_neto = f.importe_bruto
            #     ) >> \
            #     dplyr.select(
            #         f.ejercicio, f.mes, f.fecha, f.cta_cte,
            #         f.origen, f.cuit, f.beneficiario,
            #         f.movimiento, f.destino, 
            #         f.importe_bruto, f.importe_neto
            #     )
            df = pd.concat([df, banco_invico], ignore_index=True)
            # df = df >> \
            #     dplyr.bind_rows(banco_invico, _copy=False)
        self.sgf_resumen_rend_cuit = pd.DataFrame(df)
        return self.sgf_resumen_rend_cuit

    # --------------------------------------------------
    def import_resumen_rend_honorarios(self, ejercicio:str = None, dep_emb:bool = True) -> pd.DataFrame:
        df = ResumenRendProv().from_sql(self.db_path + '/sgf.sqlite')  
        df = df.loc[df['origen'] != 'OBRAS']
        df = df.loc[df['cta_cte'].isin(['130832-05', '130832-07'])]
        df = df.loc[df['destino'].isin(['HONORARIOS - FUNCIONAMIENTO', 
        'COMISIONES - FUNCIONAMIENTO', 'HONORARIOS - EPAM'])]
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
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
            banco['importe_neto'] = 0
            banco['otras'] = banco['importe_bruto']
            banco['retenciones'] = banco['importe_bruto']
            banco['destino'] = 'EMBARGO POR ALIMENTOS'
            banco['beneficiario'] = 'EMBARGO POR ALIMENTOS'
            banco.rename(columns={
                "libramiento":"libramiento_sgf",
                }, inplace=True)
            banco = banco.loc[:, [
                'ejercicio', 'mes', 'fecha', 'beneficiario',
                'destino', 'cta_cte', 'libramiento_sgf', 'movimiento',
                'importe_bruto', 'otras', 'retenciones','importe_neto']]
            df = pd.concat([df, banco])
            df = df.fillna(0)
        self.sgf_resumen_rend_honorarios = pd.DataFrame(df)
        return self.sgf_resumen_rend_honorarios

    # --------------------------------------------------
    def import_banco_invico(self, ejercicio:str = None) -> pd.DataFrame:
        df = BancoINVICO().from_sql(self.db_path + '/sscc.sqlite')
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        # if ejercicio != None:  
        #     df = df.loc[df['ejercicio'] == ejercicio]
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
        if ejercicio is not None:
            if isinstance(ejercicio, list):
                df = df.loc[df['ejercicio'].isin(ejercicio)]
            else:
                df = df.loc[df['ejercicio'].isin([ejercicio])]
        df.reset_index(drop=True, inplace=True)
        map_to = self.ctas_ctes.loc[:,['map_to', 'sscc_cta_cte']]
        df = pd.merge(
            df, map_to, how='left',
            left_on='cta_cte', right_on='sscc_cta_cte')
        df['cta_cte'] = df['map_to']
        df.drop(['map_to', 'sscc_cta_cte'], axis='columns', inplace=True)
        return df

    # --------------------------------------------------
    def import_barrios_nuevos(self, ejercicio:str = None) -> pd.DataFrame:
        df = BarriosNuevos().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio is not None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_resumen_facturado(self, ejercicio:str = None) -> pd.DataFrame:
        df = ResumenFacturado().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio is not None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_resumen_recaudado(self, ejercicio:str = None) -> pd.DataFrame:
        df = ResumenRecaudado().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio is not None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_saldo_barrio_variacion(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoBarrioVariacion().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio is not None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_saldo_barrio(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoBarrio().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio is not None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_saldo_recuperos_cobrar_variacion(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoRecuperosCobrarVariacion().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio is not None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df
    
    # --------------------------------------------------
    def import_saldo_motivo_por_barrio(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoMotivoPorBarrio().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio is not None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_saldo_motivo(self, ejercicio:str = None) -> pd.DataFrame:
        df = SaldoMotivo().from_sql(self.db_path + '/sgv.sqlite') 
        if ejercicio is not None:
            df = df.loc[df['ejercicio'] <= ejercicio]
        return df

    # --------------------------------------------------
    def import_sgo_listado_obras(self) -> pd.DataFrame:
        df = ListadoObras().from_sql(self.db_path + '/sgo.sqlite')
        df = df.loc[:, [
            'cod_obra', 'obra', 'contratista', 'localidad', 'tipo_obra',
            'operatoria', 'fecha_inicio', 'fecha_fin', 'avance_fis_real',
            'nro_ultimo_certif', 'mes_obra_certif', 'monto_pagado', 
        ]]  
        return df