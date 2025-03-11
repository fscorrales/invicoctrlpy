#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control de Banco SIIF vs Real INVICO (SSCC)
Data required:
    - SSCC Resumen General de Movimientos
    - SSCC Listado de Imputaciones
    - SSCC Saldo Final
    - SSCC ctas_ctes (manual data)
    - SIIF rcocc31 (1112-2-6 Banco)
    - SIIF rfondo07tp (PA6)
"""

__all__ = ['BancoSSCCVsSIIF']

from dataclasses import dataclass, field

import datetime as dt
import pandas as pd
from invicoctrlpy.utils import handle_path
from invicoctrlpy.utils.import_dataframe import ImportDataFrame

# --------------------------------------------------
@dataclass
class BancoSSCCVsSIIF():
    ejercicios:list[str] = None
    db_path:str = None
    siif:pd.DataFrame = field(default_factory=pd.DataFrame, init=False)
    sscc:pd.DataFrame = field(default_factory=pd.DataFrame, init=False)
    sscc_imputacion:pd.DataFrame = field(default_factory=pd.DataFrame, init=False)

    # --------------------------------------------------
    def __post_init__(self):
        if not self.ejercicios:
            self.ejercicios = str(dt.datetime.now().year)
        if not isinstance(self.ejercicios, list):
            self.ejercicios = [self.ejercicios]
        if not self.db_path:
            self.db_path = handle_path.get_db_path()

    # --------------------------------------------------
    def set_import_df_db_path(self) -> ImportDataFrame:
        import_df = ImportDataFrame()
        import_df.db_path = self.db_path
        return import_df

    # --------------------------------------------------
    def banco_sscc_con_tipo_imputacion(self) -> pd.DataFrame:
        ejercicios = sorted([str(ejercicio) for ejercicio in self.ejercicios], key=lambda x: int(x))
        import_df = self.set_import_df_db_path()
        import_df.import_ctas_ctes()
        self.sscc_imputacion = import_df.import_sscc_listado_imputaciones()
        self.sscc = import_df.import_banco_invico(ejercicio=ejercicios)
        self.sscc = self.sscc.merge(
            self.sscc_imputacion.loc[:, ['cod_imputacion', 'tipo']],
            on=['cod_imputacion'], how='left'
        )
        return self.sscc

    # --------------------------------------------------
    def banco_siif(self) -> pd.DataFrame:
        import_df = self.set_import_df_db_path()
        import_df.import_ctas_ctes()
        self.siif = import_df.import_banco_siif(ejercicio=self.ejercicios)
        # drop rows with type PAP (PA6) on tipo_comprobante field
        self.siif = self.siif[self.siif['tipo_comprobante'] != 'PAP']
        return self.siif

    # --------------------------------------------------
    def pago_retenciones_siif(self) -> pd.DataFrame:
        pass

    # --------------------------------------------------
    def retenciones_siif_vs_sscc(self) -> pd.DataFrame:
        pass

# --------------------------------------------------
if __name__ == '__main__':
    ejercicios = [str(x) for x in range(2024, 2026)]
    siif_vs_sscc = BancoSSCCVsSIIF(ejercicios=['2024'])
    siif_vs_sscc.banco_sscc_con_tipo_imputacion()
    siif_vs_sscc.banco_siif()
    print(siif_vs_sscc.sscc)

# python -m invicoctrlpy.contabilidad.banco.sscc_vs_siif