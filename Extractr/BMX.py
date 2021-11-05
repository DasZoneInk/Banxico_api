import pandas as pd
import requests
from pandas.io.json import json_normalize


class Banxico:
    def __init__(self, token):
        self.token = token

    def get_metadata(self, series_id: list, eng=True) -> pd.DataFrame:
        """
        Fetch metadata from Banxico Series APIRest
        Args:
            acces_token: Token str for access
            series_id: list for series (max len = 20)
            eng: results should be displayed in english?
        """
        endpoint = f'https://www.banxico.org.mx/SieAPIRest/service/v1/series/{",".join(x for x in series_id)}'

        with requests.Session() as s:
            request = s.get(
                endpoint,
                headers={"Accept": "application/json"},
                params={
                    "token": self.token,
                    "locale": ("en" if eng else "es"),
                    "mediaType": "json",
                },
            )
        file = request.json()
        if request.status_code == 200:
            file = pd.DataFrame(file["bmx"]["series"])

        else:
            print(
                f"Returned None: There is an error in series_id ({file['error']['mensaje']}){file['error']['detalle']}"
            )
            file = None
        return file

    def get_data(
        self,
        series_id: list,
        fechas: list,
        decimales=False,
        tip_increm=None,
        oportuno=False,
        eng=True,
    ) -> pd.DataFrame:
        """
        Fetch all data from Banxico Series APIRest
        fechas: lista de dos valores
            [0] Fecha Inicial: formato yyyy-MM-dd
            [1] Fecha Final: formato yyyy-MM-dd
        tip_increm:
            None: Niveles
            1: incremento respecto a la observacion anterior
            2: incremento respecto al mismo periodo del año anterior
            3: respecto de la ultima observacion del año anterior
        """
        if oportuno & (fechas is None):
            compl = "/oportuno" if oportuno else ""
        elif oportuno & (fechas is not None):
            print("overriding Param Fecha for oportuno")
            compl = "/oportuno" if oportuno else ""
        elif (not oportuno) & (fechas is not None):
            compl = f"/{fechas[0]}/{fechas[1]}"
        else:
            compl = ""

        print(compl)
        endpoint = f'https://www.banxico.org.mx/SieAPIRest/service/v1/series/{",".join(x for x in series_id)}/datos{compl}'

        incremento = {1: "PorcObsAnt", 2: "PorcAnual", 3: "PorcAcumAnual"}

        with requests.Session() as s:
            request = s.get(
                endpoint,
                headers={"Accept": "application/json"},
                params={
                    "token": self.token,
                    "locale": ("en" if eng else "es"),
                    "mediaType": "json",
                    "incremento": incremento.get(tip_increm),
                },
            )
        file = request.json()
        df_container = []
        if request.status_code == 200:
            file = pd.DataFrame(file["bmx"]["series"])
            for i in range(len(series_id)):
                print(f'Normalizing data frame {i} series: {file["idSerie"][i]}')

                aux_df = json_normalize(file["datos"][i])
                aux_df["series_name"], aux_df["series_code"] = (
                    file["titulo"][i],
                    file["idSerie"][i],
                )
                aux_df["fecha"] = pd.to_datetime(
                    aux_df["fecha"], format="%d/%m/%Y"
                ).apply(lambda x: x.date().isoformat())
                aux_df["dato"] = (
                    aux_df["dato"].apply(lambda x: x.replace(",", "")).astype(float)
                )

                df_container.append(aux_df)

        else:
            print(request.url)
            print(request.status_code)
            print(
                f"Returned None: There is an error in series_id ({file['error']['mensaje']}){file['error']['detalle']}"
            )
            df_container = None
        return df_container  # Mapping type
