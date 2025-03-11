#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gamail.com>
Date   : 07-Mar-2025
Purpose: Cálculo de Remanente y Mal Llamado Remanente
Data required:
    - SIIF rf602
    - SIIF rf610
    - SIIF rci02
    - SIIF rdeu012
    - SIIF rdeu012bc_c (Pedir a Tesorería General de la Provincia)
    - SSCC ctas_ctes (manual data)
    - Saldos por cuenta Banco INVICO (SSCC) al 31/12 de cada año (SSCC-Cuentas-Resumen Gral de Saldos)
    - Icaro (¿sólo para fondos de reparo? ¿Vale la pena?)
"""

import argparse
import datetime as dt
import inspect
import os

import pandas as pd
from dataclasses import dataclass

from invicoctrlpy.utils import handle_path
from invicoctrlpy.utils.import_dataframe import ImportDataFrame


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Remanente y Mal Llamado Remanente",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "ejercicio",
        metavar="ejercicio",
        default=[dt.datetime.now().year],
        type=int,
        choices=range(2010, dt.datetime.now().year + 1),
        help="Ejercicio Remamente",
    )

    return parser.parse_args()

@dataclass
class Remanente():
    ejercicio: str = None
    db_path: str = None

    # --------------------------------------------------
    def __post_init__(self):
        if not self.ejercicio:
            self.ejercicio = str(dt.datetime.now().year)
        if not self.db_path:
            self.db_path = handle_path.get_db_path()
        self.import_df = ImportDataFrame()
        self.import_df.db_path = self.db_path
        self.import_df.import_ctas_ctes()
        

    # --------------------------------------------------
    def import_sdo_final_banco_invico(self) -> pd.DataFrame:
        df = self.import_df.import_sdo_final_banco_invico(ejercicio=self.ejercicio)
        return df


    # --------------------------------------------------
    def hoja_trabajo(self) -> pd.DataFrame:
        df = self.import_df.import_siif_rf602(ejercicio=self.ejercicio)
        df = df.loc[df["partida"].isin(["421", "422"])]
        df = df.loc[df["fuente"] != "11"]
        df["estructura"] = df["estructura"].str[:-4]
        df_desc = self.import_df.import_icaro_desc_pres()
        df_desc.rename(
            columns={
                "actividad": "estructura",
                "desc_prog": "prog_con_desc",
                "desc_subprog": "subprog_con_desc",
                "desc_proy": "proy_con_desc",
                "desc_act": "act_con_desc",
            },
            inplace=True,
        )
        df = df.merge(df_desc, how="left", on="estructura", copy=False)
        df.drop(
            [
                "grupo",
                "credito_original",
                "comprometido",
                "pendiente",
                "programa",
                "subprograma",
                "proyecto",
                "actividad",
            ],
            axis=1,
            inplace=True,
        )
        df["remte"] = 0
        df["mal_remte"] = 0
        df["saldo_remte"] = df["saldo"]
        df["cc_remte"] = None
        df["cc_mal_remte"] = None
        df["f_y_f"] = None
        return df

    # --------------------------------------------------
    def deuda_flotante(self) -> pd.DataFrame:
        rdeu = self.import_df.import_siif_rdeu012(ejercicio=self.ejercicio)
        months = rdeu["mes_hasta"].tolist()
        # Convertir cada elemento de la lista a un objeto datetime
        dates = [dt.datetime.strptime(month, "%m/%Y") for month in months]
        # Obtener la fecha más reciente y Convertir la fecha mayor a un string en el formato 'MM/YYYY'
        gt_month = max(dates).strftime("%m/%Y")
        rdeu = rdeu.loc[rdeu["mes_hasta"] == gt_month, :]
        return rdeu

    # --------------------------------------------------
    def deuda_flotante_tg(self) -> pd.DataFrame:
        return self.import_df.import_siif_rdeu012b2_c(mes_hasta = '12/' + self.ejercicio) 

    # --------------------------------------------------
    def remanente_met_1(self):
        """
        El cálculo de Remanente por método I consiste en restarle al saldo real de Banco SSCC 
        (Resumen Gral de Saldos), al cierre del ejercicio, la deuda flotante (reporte SIIF 
        rdeu012) del SIIF al 31/12 del mismo ejercicio. Tener en cuenta las siguiente 
        consideraciones:
        - Las fuentes 10 y 12 del SIIF son ejecutadas con las cuentas corrientes 130832-03 
        y 130832-06
        - La fuente 13 del SIIF es ejecutada con la cuenta corriente 130832-16 a lo que hay 
        que adicionarle 4.262.062,77 que quedó de saldo transferido por la UCAPFI en 09/2019 
        en la cuenta 22101105-48 del programa EPAM Habitat.
        - El resto del Saldo Banco SSCC corresponde a la fuente 11
        - Tener en cuenta los ajustes contables que buscan regularizar comprobantes del SIIF 
        que quedaron en la Deuda Flotante y no deben formar parte de la misma.
        """
        SALDO_UCAPFI = 4262062.77
        banco_sscc = self.import_sdo_final_banco_invico()
        rdeu = self.deuda_flotante()
        rem_met_1 = {
            "Fuente 10 y 12": {
                "saldo_bco": banco_sscc.loc[
                    banco_sscc["cta_cte"].isin(["130832-03"])
                ]["saldo"].sum(),
                "rdeu": rdeu.loc[rdeu.fuente.isin(["10", "12"])]["saldo"].sum(),
            },
            "Fuente 13": {
                "saldo_bco": banco_sscc.loc[banco_sscc["cta_cte"] == "130832-16"][
                    "saldo"
                ].sum()
                + SALDO_UCAPFI,
                "rdeu": rdeu.loc[rdeu.fuente.isin(["13"])]["saldo"].sum(),
            },
            "Fuente 11": {
                "saldo_bco": banco_sscc.loc[
                    ~banco_sscc["cta_cte"].isin(["130832-03", "130832-16"])
                ]["saldo"].sum()
                - SALDO_UCAPFI,
                "rdeu": rdeu.loc[rdeu.fuente.isin(["11"])]["saldo"].sum(),
            },
        }
        rem_met_1 = pd.DataFrame.from_dict(rem_met_1, orient="index")
        rem_met_1.reset_index(inplace=True)
        rem_met_1.columns = ["fuente", "saldo_bco", "rdeu"]
        rem_met_1["rte_met_1"] = rem_met_1.saldo_bco - rem_met_1.rdeu
        return rem_met_1


    # --------------------------------------------------
    def remanente_met_2(self):
        """
        El cálculo de Remanente por método II consiste en restarle a los Recursos SIIF 
        (reporte SIIF rci02), ingresados en el ejercicio bajo análisis, los Gastos SIIF 
        (reporte SIIF rf602) de dicho ejercicio. Tener en cuenta los ajustes contables 
        que buscan regularizar comprobantes del SIIF que quedaron en la Deuda Flotante 
        y no deben formar parte de la misma.
        """
        recursos = self.import_df.import_siif_rci02(ejercicio=self.ejercicio)
        gastos = self.import_df.import_siif_rf602(ejercicio=self.ejercicio)
        rem_met_2 = recursos.importe.groupby([recursos.fuente]).sum()
        rem_met_2 = pd.concat(
            [
                rem_met_2,
                gastos.ordenado.groupby([gastos.fuente]).sum(),
                gastos.saldo.groupby([gastos.fuente]).sum(),
            ],
            axis=1,
        )
        rem_met_2.columns = ["recursos", "gastos", "saldo_pres"]
        rem_met_2["rte_met_2"] = rem_met_2.recursos - rem_met_2.gastos
        rem_met_2.reset_index(inplace=True)
        # rem_met_2 = rem_met_2[~rem_met_2.index.isin(['11'], level=1)]
        # rem_met_2.dropna(inplace=True)
        return rem_met_2

    # --------------------------------------------------
    def remanente_met_2_hist(self):
        recursos = self.import_df.import_siif_rci02()
        gastos = self.import_df.import_siif_rf602()
        rem_solicitado = recursos.loc[recursos.es_remanente == True].importe.groupby([recursos.ejercicio, recursos.fuente]).sum().to_frame()
        rem_solicitado.reset_index(inplace=True)
        rem_solicitado['ejercicio'] = (rem_solicitado['ejercicio'].astype(int) - 1).astype(str)
        rem_met_2_hist = recursos.importe.groupby([recursos.ejercicio, recursos.fuente]).sum()
        rem_met_2_hist = pd.concat([rem_met_2_hist, gastos.ordenado.groupby([gastos.ejercicio, gastos.fuente]).sum(),
        gastos.saldo.groupby([gastos.ejercicio, gastos.fuente]).sum()], axis=1)
        rem_met_2_hist.reset_index(inplace=True)
        rem_met_2_hist = rem_met_2_hist.merge(rem_solicitado, how='left', on=['ejercicio', 'fuente'], copy=False)
        rem_met_2_hist.columns = ["ejercicio", "fuente", "recursos", "gastos", "saldo_pres", "rte_solicitado"]
        rem_met_2_hist["rte_met_2"] = rem_met_2_hist.recursos - rem_met_2_hist.gastos
        # rem_met_2_hist["mal_rte_met_2"] = rem_met_2_hist.saldo_pres- rem_met_2_hist.rte_met_2
        rem_met_2_hist["dif_rte_solicitado"] = rem_met_2_hist.rte_solicitado - rem_met_2_hist.rte_met_2
        rem_met_2_hist = rem_met_2_hist[~rem_met_2_hist.fuente.isin(['11'])]
        #No sé qué pasó en Fuente 13 en el 2013, por eso lo filtro
        #rem_met_2_hist = rem_met_2_hist[~rem_met_2_hist.index.isin(['2011', '2012', '2013'], level=0)]
        #rem_met_2_hist.reset_index(inplace=True)
        rem_met_2_hist.dropna(inplace=True)
        return rem_met_2_hist

    # --------------------------------------------------
    def remanente_dif_met(self):
        rem_met_1 = self.remanente_met_1()
        rem_met_1 = rem_met_1.loc[:, ['fuente', 'rte_met_1']]
        rem_met_2 = self.remanente_met_2()
        rem_met_2 = rem_met_2.loc[:, ['fuente', 'rte_met_2']]
        # rem_met_2 = rem_met_2.loc[~rem_met_2['fuente'].isin(['10', '12'])]
        rem_met_2 = pd.DataFrame([
            ['Fuente 10 y 12', rem_met_2.loc[rem_met_2['fuente'].isin(['10', '12'])]['rte_met_2'].sum()],
            ['Fuente 11', rem_met_2.loc[rem_met_2['fuente'] == '11']['rte_met_2'].sum()],
            ['Fuente 13', rem_met_2.loc[rem_met_2['fuente'] == '13']['rte_met_2'].sum()]
        ], columns=['fuente', 'rte_met_2'])
        rem_met = rem_met_1.merge(rem_met_2, how='left', on='fuente')
        rem_met['dif_metodos'] = rem_met.rte_met_1 - rem_met.rte_met_2
        return rem_met


# --------------------------------------------------
def main():
    """Make a jazz noise here"""

    args = get_args()
    ejercicio = str(args.ejercicio)

    print(f'Ejercicio = "{ejercicio}"')

    save_path = os.path.dirname(
        os.path.abspath(inspect.getfile(inspect.currentframe()))
    )
    file_name = os.path.join(save_path, "Remanente " + ejercicio + ".xlsx")
    with pd.ExcelWriter(file_name) as writer:
        remanente = Remanente(ejercicio=ejercicio)
        remanente.hoja_trabajo().to_excel(
            writer, sheet_name="HojaTrabajoFdoProv", index=False
        )
        remanente.remanente_met_1().to_excel(
            writer, sheet_name="RemMet1", index=False
        )
        remanente.remanente_met_2().to_excel(
            writer, sheet_name="RemMet2", index=False
        )
        remanente.remanente_met_2_hist().to_excel(writer, sheet_name="RemMet2Hist", index=False)
        remanente.remanente_dif_met().to_excel(writer, sheet_name="RemDifMet", index=False)
        remanente.deuda_flotante().to_excel(writer, sheet_name="Rdeu", index=False)
        remanente.deuda_flotante_tg().to_excel(writer, sheet_name="RdeuTG", index=False)
        remanente.import_sdo_final_banco_invico().to_excel(
            writer, sheet_name="SldBcoCierre", index=False
        )
        # siif_ppto_con_desc.to_excel(writer, sheet_name='HojaTrabajoFdoProv', index=False)


# --------------------------------------------------
if __name__ == "__main__":
    main()

# python -m src.invicoctrlpy.remanente.remanente
