import pandas as pd
from pathlib import Path
import numpy as np
import os


def load_circuit_data(circuidnrs, df: pd.DataFrame, path: str):
    """
    Load the csv data (propagation, sensitively & pd) from the circuit directorys (and resamples PD to 60min)
    :param circuidnrs: List of circuits to load
    :param df: dataframe object containing (at least) the time range
    :param path: Data directory path
    :return dataframe with added column(s)
    """
    for circnr in circuidnrs:
        circ_dir = Path(path) / str(circnr)

        df1 = pd.read_csv(circ_dir / 'Propagation.csv', sep=';', parse_dates=['Date/time (UTC)'])
        df1 = df1.rename(columns={"Propagation time (ns)": "Propagation time (ns) " + str(circnr)})

        df2 = pd.read_csv(circ_dir / 'Sensitivity.csv', sep=';', parse_dates=['Date/time (UTC)'])
        df2 = df2.rename(columns={"PD Detection Sensitivity (pC)": "PD Detection Sensitivity (pC) " + str(circnr)})

        df3 = pd.read_csv(circ_dir / 'PD.csv', sep=';', parse_dates=['Date/time (UTC)'])
        df3 = df3.drop("Location in meters (m)", axis=1)
        df3 = df3.rename(columns={"Charge (picocoulomb)": "Total charge (pC) " + str(circnr)})
        df3_res = df3.resample('60min', on='Date/time (UTC)').sum()

        df = pd.merge(df, df1, on="Date/time (UTC)", how='left')
        df = pd.merge(df, df2, on="Date/time (UTC)", how='left')
        df = pd.merge(df, df3_res, on="Date/time (UTC)", how='left')

    return df


def load_wop_data(df: pd.DataFrame, path: str):
    """
    Load the csv data (power, current & voltage) from WOP datadump and resample to 60min
    :param df: dataframe object containing (at least) the time range
    :param path: Data directory path
    :return dataframe with added column(s)
    """
    power_df = pd.read_csv(Path(path) / 'Power.csv', sep=';', parse_dates={'Date/time (UTC)': [' Datum', 'Tijd']},
                           date_parser=(lambda x: pd.to_datetime(x, format="%Y/%m/%d %H:%M")), decimal=',')
    power_df.drop(power_df.columns[power_df.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
    power_df_res = power_df.resample('60min', on='Date/time (UTC)').sum()
    return pd.merge(df, power_df_res, on="Date/time (UTC)", how='left')


def load_temp_data(df: pd.DataFrame, path: str):
    """
    Load the csv data (temperature),
    :param df: dataframe object containing (at least) the time range
    :param path: Data directory path
    :return dataframe with added column(s)
    """
    temp_df = pd.read_csv(Path(path) / 'Temperature.csv', sep=';', parse_dates={'Date/time (UTC)': ['time']})
    soiltemp_df = pd.read_csv(Path(path) / 'SoilTemperature.csv', sep=';', parse_dates={'Date/time (UTC)': ['time']})
    total_temps_df = pd.merge(temp_df, soiltemp_df, on="Date/time (UTC)", how='left')
    return pd.merge(df, total_temps_df, on="Date/time (UTC)", how='left')
