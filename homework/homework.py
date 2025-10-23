"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import os
import glob
import pandas as pd

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerles un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cambiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """

    def create_empty_df():
        """Crea un dataframe vacio con la cabecera adecuada para hacer concat"""

        columnas = ("client_id,age,job,marital,education,credit_default,"
        "mortgage,month,day,contact_duration,number_contacts,previous_campaign_contacts,"
        "previous_outcome,cons_price_idx,euribor_three_months,campaign_outcome").split(",")

        df = pd.DataFrame(columns=columnas)
        df.index.name = ""

        return df

    def read_files_and_concat(path, df):
        """Concatena los csv de la campaña de marketing al df vacio"""

        files = glob.glob(f"{path}*")
        for file in files:
            df_temp = pd.read_csv(file, index_col=0)
            df = pd.concat([df, df_temp], axis=0, ignore_index=True)

        return df

    def create_and_clean_client_df(df):
        """Crea y limpia el df con la data necesaria para client.csv"""

        df_client = df.copy().iloc[:, :7]
        df_client["job"] = (
            df_client["job"].str.replace(".", "", regex=False)
            .str.replace("-", "_")
        )
        df_client["education"] = (
            df_client["education"].str.replace(".", "_")
            .replace("unknown", pd.NA)
        )
        df_client["credit_default"] = (
            df_client["credit_default"]
            .apply(lambda x: 1 if x == "yes" else 0)
        )
        df_client["mortgage"] = (
            df_client["mortgage"]
            .apply(lambda x: 1 if x == "yes" else 0)
        )

        return df_client

    def create_and_clean_campaign_df(df):
        """Crea y limpia el df con la data necesaria para campaign.csv"""

        df_campaign = df.copy().iloc[:, [0] + list(range(7, 13)) + [-1]]
        df_campaign["previous_outcome"] = (
            df_campaign["previous_outcome"]
            .apply(lambda x: 1 if x == "success" else 0)
        )
        df_campaign["campaign_outcome"] = (
            df_campaign["campaign_outcome"]
            .apply(lambda x: 1 if x == "yes" else 0)
        )
        df_campaign["last_contact_date"] = (
            pd.to_datetime("2022-" + df_campaign["month"].astype(str)
                        + "-" + df_campaign["day"].astype(str))
        )
        df_campaign.drop(["day", "month"], axis=1, inplace=True)

        return df_campaign

    def create_economics_df(df):
        """Crea el df con la data necesaria para campaign.csv"""

        df_economics = df.copy().iloc[:, [0, -3, -2]]

        return df_economics

    def save_csv(df, output_path):
        """Escribe el dataframe en la carpeta de output_path"""

        df.to_csv(f"{output_path}", index=False, header=True)

    def makedir(output_path):
        """Crea la carpeta output"""

        if os.path.exists(output_path):
            for file in glob.glob(f"{output_path}*"):
                os.remove(file)
        else:
            os.makedirs(output_path)

    files_path = "files/input/"
    output_path = "files/output/"
    makedir(output_path)
    df = create_empty_df()
    df = read_files_and_concat(files_path, df)
    df_client = create_and_clean_client_df(df)
    df_campaign = create_and_clean_campaign_df(df)
    df_economics = create_economics_df(df)
    dataframes = [df_client, df_campaign, df_economics]
    output_paths = [
        "files/output/client.csv",
        "files/output/campaign.csv",
        "files/output/economics.csv",
    ]

    for df_part, path in zip(dataframes, output_paths):
        save_csv(df_part, path)

if __name__ == "__main__":
    clean_campaign_data()
