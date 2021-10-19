#!/usr/bin/env python3

# Quick process TrakCare Monitor Data to collate and visualise some interesting metrics
# Source data must be exported from the TrakCare Monitor Tool

# Example usage: TrakCare_Monitor.py -d directory [-l list of databases to include/exclude from episode size] -g
#                Globals take a minute or so to process, explicitly exclude with -g
# example: TrakCare_Monitor.py -d site_monitor -l TRAK-DOCUMENT TRAK-MONITOR

import os
import sys

import logging

import pandas as pd
import matplotlib as mpl

# mpl.use("TkAgg")
import seaborn as sns

from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from matplotlib import ticker
from matplotlib.dates import MO

import numpy as np
import glob
import argparse

from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()


# Generic plot by date, single line, ticks on a Monday.


def generic_plot(
    df,
    column,
    title,
    y_label,
    save_as,
    pres=False,
    y_zero=True,
    plot_text_string="",
    plot_hours=False,
):
    colormap_name = "Set1"

    plt.style.use("seaborn-whitegrid")
    plt.figure(num=None, figsize=(10, 6), dpi=300)
    palette = plt.get_cmap(colormap_name)
    color = palette(1)

    plt.plot(df[column], color=color, alpha=0.7)
    plt.title(title, fontsize=14)
    plt.ylabel(y_label, fontsize=10)
    plt.tick_params(labelsize=10)

    ax = plt.gca()
    ax.grid(which="major", axis="both", linestyle="--")

    if y_zero:
        ax.set_ylim(bottom=0)  # Always zero start
    if pres:
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.2f}"))
    else:
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))

    if plot_hours:
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %H:%M"))
    else:
        ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.WeekdayLocator(byweekday=MO)))

    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    plt.text(
        0.01,
        0.95,
        plot_text_string,
        ha="left",
        va="center",
        transform=ax.transAxes,
        fontsize=12,
    )
    plt.tight_layout()
    plt.savefig(save_as, format="png")
    plt.close()


def generic_top_n(df_sort, top_n, df_master_ps, plot_what, title, y_label, save_as, pres=False):
    colormapName = "Set1"

    top_List = df_sort["pName"].head(top_n).tolist()
    grpd = df_master_ps.groupby("pName")

    plt.style.use("seaborn-whitegrid")
    plt.figure(num=None, figsize=(10, 6), dpi=300)
    palette = plt.get_cmap(colormapName)
    color = palette(1)

    current_palette_10 = sns.color_palette("Paired", top_n)
    sns.set_palette(current_palette_10)

    for name, data in grpd:
        if name in top_List:
            plt.plot(data.Date.values, data.eval(plot_what).values, "-", label=name)
    plt.title(title, fontsize=14)
    plt.ylabel(y_label, fontsize=10)
    plt.tick_params(labelsize=10)
    plt.legend(loc="upper left")
    ax = plt.gca()
    ax.set_ylim(bottom=0)  # Always zero start
    if pres:
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.2f}"))
    else:
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))

    ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.WeekdayLocator(byweekday=MO)))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(save_as, format="png")
    plt.close()


# Dont crowd the pie chart. To do; bucket 'Other' after 2pct


def make_autopct(values):
    def my_autopct(pct):
        total = sum(values)
        val = int(round(pct * total / 102400.0))
        return "{p:.0f}%  ({v:,d} GB)".format(p=pct, v=val) if pct > 2 else ""

    return my_autopct


def average_episode_size(DIRECTORY, MonitorAppFile, MonitorDatabaseFile, TRAKDOCS, INCLUDE):
    logger = logging.getLogger(__name__)
    colormapName = "Set1"

    # Get the episode data
    outputName = os.path.splitext(os.path.basename(MonitorDatabaseFile))[0]
    outputFile_png = DIRECTORY + "/all_out_png/" + outputName + "_Summary"
    outputFile_csv = DIRECTORY + "/all_out_csv/" + outputName + "_Summary"
    print("Episode size: %s" % outputName)

    df_master_ep = pd.read_csv(MonitorAppFile, sep="\t", encoding="ISO-8859-1")

    # EpisodeCountEmergency column is empty() in some versions of TC
    emergency_empty = False
    if pd.isna(df_master_ep["EpisodeCountEmergency"]).all():
        emergency_empty = True
    lab_empty = False
    if pd.isna(df_master_ep["LabEpisodeCountTotal"]).all():
        lab_empty = True

    df_master_ep = df_master_ep.dropna(axis=1, how="all")
    df_master_ep = df_master_ep.rename(columns={"RunDate": "Date"})

    # Cut down to just what we care about
    # print(f"\nTEST\n")
    # print(f"\n{df_master_ep.to_string()}\n")

    columns = ["Date", "RunTime", "EpisodeCountTotal", "EpisodeCountInpatient", "EpisodeCountOutpatient"]
    if not emergency_empty:
        columns.append("EpisodeCountEmergency")
    if not lab_empty:
        columns.append("LabEpisodeCountTotal")

    df_master_ep = df_master_ep[columns]

    # print(f"\nDatabase\n{df_master_ep}")
    # Get the database growth data
    df_master_db = pd.read_csv(MonitorDatabaseFile, sep="\t", encoding="ISO-8859-1")
    df_master_db = df_master_db.dropna(axis=1, how="all")
    df_master_db = df_master_db.rename(columns={"RunDate": "Date"})

    # Calculate actual database used
    df_master_db["DatabaseUsedMB"] = df_master_db["SizeinMB"] - df_master_db["FreeSpace"]
    df_master_db = df_master_db[["Date", "DatabaseUsedMB", "Name"]]

    df_master_db.to_csv(outputFile_csv + "Database_With_Docs.csv", sep=",", index=False)

    # Always exclude CACHETEMP
    df_master_db = df_master_db[df_master_db.Name != "CACHETEMP"]

    # If all databases including docs
    if TRAKDOCS == ["all"]:
        includew = " with "
        df_master_db_dm = df_master_db
        outputFile_png_x = outputFile_png + "_All_EP_Size.png"
    else:
        # INCLUDE only the document database ? = True
        if INCLUDE:
            includew = " only "
            df_master_db_dm = df_master_db[df_master_db["Name"].isin(TRAKDOCS)]
            outputFile_png_x = outputFile_png + "_" + "_".join(TRAKDOCS) + "_EP_Size.png"
        # All databases except document database
        else:
            includew = " without "
            df_master_db_dm = df_master_db[~df_master_db["Name"].isin(TRAKDOCS)]
            outputFile_png_x = outputFile_png + "_Not_" + "_".join(TRAKDOCS) + "_EP_Size.png"

    # Group databases by date, add column for growth per day, remove date index for merging
    df_db_by_date = df_master_db_dm.groupby("Date").sum()

    df_db_by_date["DatabaseGrowthMB"] = df_db_by_date["DatabaseUsedMB"] - df_db_by_date["DatabaseUsedMB"].shift(1)
    df_db_by_date = df_db_by_date[np.isfinite(df_db_by_date["DatabaseGrowthMB"])]
    df_db_by_date.reset_index(level=0, inplace=True)

    # Merge episodes and database growth on date, create column for daily plot
    df_result = pd.merge(df_master_ep, df_db_by_date)
    df_result["AvgEpisodeSizeMB"] = df_result["DatabaseGrowthMB"] / df_result["EpisodeCountTotal"]
    df_result["Date"] = pd.to_datetime(df_result["Date"])
    df_result.set_index("Date", inplace=True)

    if TRAKDOCS == ["all"]:
        df_result.to_csv(outputFile_csv + "Database_Growth.csv", sep=",", index=True)

    # Build the plot
    # print(f"\nDatabase\n{df_result}")

    DatabaseGrowthTotal = df_result.iloc[-1]["DatabaseUsedMB"] - df_result.iloc[0]["DatabaseUsedMB"]
    TotalEpisodes = df_result["EpisodeCountTotal"].sum()
    AverageEpisodeSize = round(DatabaseGrowthTotal / TotalEpisodes, 2)
    TextString = "Average growth/episode{}{}: {} MB".format(includew, ", ".join(TRAKDOCS), AverageEpisodeSize)

    RunDateStart = df_result.head(1).index.tolist()
    RunDateStart = RunDateStart[0].strftime("%d/%m/%Y")
    RunDateEnd = df_result.tail(1).index.tolist()
    RunDateEnd = RunDateEnd[0].strftime("%d/%m/%Y")

    plt.style.use("seaborn-whitegrid")
    plt.figure(num=None, figsize=(10, 6), dpi=300)
    palette = plt.get_cmap(colormapName)
    color = palette(1)

    plt.plot(df_result["AvgEpisodeSizeMB"])
    plt.title("Average Episode Size " + RunDateStart + " - " + RunDateEnd, fontsize=14)
    plt.tick_params(labelsize=10)
    ax = plt.gca()
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.2f}"))
    ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.WeekdayLocator(byweekday=MO)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m/%y - %H:%M"))
    plt.ylabel("Average episode size (MB)", fontsize=10)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    plt.text(
        0.01,
        0.95,
        TextString,
        ha="left",
        va="center",
        transform=ax.transAxes,
        fontsize=12,
    )
    plt.tight_layout()
    plt.savefig(outputFile_png_x, format="png")
    # plt.show()
    plt.close()

    # Print some useful stats to txt file
    # Note on individual days there will be rounding errors of MBs
    #          - for totals use start and end figures where possible, eg end-start not growth_column.sum()

    # print(f"\n\nResults: {df_result}")

    if TRAKDOCS == ["all"]:
        with open(DIRECTORY + "/all_" + outputName + "_Basic_Stats.txt", "w") as f:
            f.write(
                "Number of days data            : " + "{v:,.0f}".format(v=df_result["DatabaseUsedMB"].count()) + "\n"
            )
            f.write(
                "Database size at start         : "
                + "{v:,.0f}".format(v=df_result.iloc[0]["DatabaseUsedMB"] / 1024)
                + " GB\n"
            )
            f.write(
                "Database size at end           : "
                + "{v:,.0f}".format(v=df_result.iloc[-1]["DatabaseUsedMB"] / 1024)
                + " GB\n"
            )

            f.write("\nTotal database growth          : " + "{v:,.3f}".format(v=DatabaseGrowthTotal / 1024) + " GB\n")
            f.write(
                "Peak database growth/day       : "
                + "{v:,.3f}".format(v=df_result["DatabaseGrowthMB"].max() / 1024)
                + " GB\n"
            )
            f.write(
                "Average database growth/day    : "
                + "{v:,.3f}".format(v=(DatabaseGrowthTotal / 1024) / df_result["DatabaseGrowthMB"].count())
                + " GB\n"
            )
            f.write(
                "Estimated database growth/year : "
                + "{v:,.0f}".format(v=((DatabaseGrowthTotal / 1024) / df_result["DatabaseGrowthMB"].count()) * 365)
                + " GB\n\n"
            )

            f.write(
                "Sum episodes                   : " + "{v:,.0f}".format(v=df_result["EpisodeCountTotal"].sum()) + "\n"
            )
            f.write(
                "Average episodes/day           : " + "{v:,.0f}".format(v=df_result["EpisodeCountTotal"].mean()) + "\n"
            )
            f.write(
                "Peak episodes/day              : " + "{v:,.0f}".format(v=df_result["EpisodeCountTotal"].max()) + "\n"
            )
            f.write(
                "Estimated episodes/year        : "
                + "{v:,.0f}".format(v=df_result["EpisodeCountTotal"].mean() * 365)
                + "\n\n"
            )

            # Emergency
            if not emergency_empty:
                f.write(
                    "Sum Emergency episodes                   : "
                    + "{v:,.0f}".format(v=df_result["EpisodeCountEmergency"].sum())
                    + "\n"
                )
                f.write(
                    "Average Emergency episodes/day           : "
                    + "{v:,.0f}".format(v=df_result["EpisodeCountEmergency"].mean())
                    + "\n"
                )
                f.write(
                    "Peak Emergency episodes/day              : "
                    + "{v:,.0f}".format(v=df_result["EpisodeCountEmergency"].max())
                    + "\n"
                )
                f.write(
                    "Estimated Emergency episodes/year        : "
                    + "{v:,.0f}".format(v=df_result["EpisodeCountEmergency"].mean() * 365)
                    + "\n\n"
                )

            # Inpatient
            f.write(
                "Sum Inpatient episodes                   : "
                + "{v:,.0f}".format(v=df_result["EpisodeCountInpatient"].sum())
                + "\n"
            )
            f.write(
                "Average Inpatient episodes/day           : "
                + "{v:,.0f}".format(v=df_result["EpisodeCountInpatient"].mean())
                + "\n"
            )
            f.write(
                "Peak Inpatient episodes/day              : "
                + "{v:,.0f}".format(v=df_result["EpisodeCountInpatient"].max())
                + "\n"
            )
            f.write(
                "Estimated Inpatient episodes/year        : "
                + "{v:,.0f}".format(v=df_result["EpisodeCountInpatient"].mean() * 365)
                + "\n\n"
            )
            # Outpatient
            f.write(
                "Sum Outpatient episodes                   : "
                + "{v:,.0f}".format(v=df_result["EpisodeCountOutpatient"].sum())
                + "\n"
            )
            f.write(
                "Average Outpatient episodes/day           : "
                + "{v:,.0f}".format(v=df_result["EpisodeCountOutpatient"].mean())
                + "\n"
            )
            f.write(
                "Peak Outpatient episodes/day              : "
                + "{v:,.0f}".format(v=df_result["EpisodeCountOutpatient"].max())
                + "\n"
            )
            f.write(
                "Estimated Outpatient episodes/year        : "
                + "{v:,.0f}".format(v=df_result["EpisodeCountOutpatient"].mean() * 365)
                + "\n\n"
            )

            # # Lab
            if not lab_empty:
                f.write(
                    "Sum Lab episodes                          : "
                    + "{v:,.0f}".format(v=df_result["LabEpisodeCountTotal"].sum())
                    + "\n"
                )
                f.write(
                    "Average Lab episodes/day                  : "
                    + "{v:,.0f}".format(v=df_result["LabEpisodeCountTotal"].mean())
                    + "\n"
                )
                f.write(
                    "Peak Lab episodes/day                     : "
                    + "{v:,.0f}".format(v=df_result["LabEpisodeCountTotal"].max())
                    + "\n"
                )
                f.write(
                    "Estimated Lab episodes/year               : "
                    + "{v:,.0f}".format(v=df_result["LabEpisodeCountTotal"].mean() * 365)
                    + "\n\n"
                )

            f.write(
                "Total database growth{0}{1} databases: {2:,.3f}".format(
                    includew, ", ".join(TRAKDOCS), DatabaseGrowthTotal / 1024
                )
                + " GB\n"
            )
            f.write(
                "Average growth/episode{0}{1} databases: {2:,.0f} KB (per episode size)".format(
                    includew, ", ".join(TRAKDOCS), AverageEpisodeSize * 1024
                )
                + "\n"
            )

            TextString = (
                "Database size at end : " + "{v:,.0f}".format(v=df_result.iloc[-1]["DatabaseUsedMB"] / 1024) + " GB\n"
            )
            generic_plot(
                df_result,
                "DatabaseUsedMB",
                "Total Database Size (MB)  " + RunDateStart + " to " + RunDateEnd,
                "MB",
                outputFile_png + "_All_Total.png",
                False,
                True,
                TextString,
            )
            TextString = (
                "Average database growth/day : "
                + "{v:,.3f}".format(v=DatabaseGrowthTotal / 1024 / df_result["DatabaseGrowthMB"].count())
                + " GB"
            )
            generic_plot(
                df_result,
                "DatabaseGrowthMB",
                "Database Growth per Day (MB)  " + RunDateStart + " to " + RunDateEnd,
                "MB",
                outputFile_png + "_All_Growth.png",
                False,
                False,
                TextString,
            )
    else:
        with open(DIRECTORY + "/all_" + outputName + "_Basic_Stats.txt", "a") as f:
            f.write(
                "\nTotal database growth{0}{1}: {2:,.2f}".format(
                    includew, ", ".join(TRAKDOCS), DatabaseGrowthTotal / 1024
                )
                + " GB\n"
            )
            f.write(
                "Average growth/episode{0}{1}: {2:,.0f} KB (per episode size)".format(
                    includew, ", ".join(TRAKDOCS), AverageEpisodeSize * 1024
                )
                + "\n"
            )

            ChartTitle = (
                "Total Database Size (MB)" + includew + ", ".join(TRAKDOCS) + " " + RunDateStart + " to " + RunDateEnd
            )
            if INCLUDE:
                outputFile_png_y = outputFile_png + "_" + "_".join(TRAKDOCS) + "_Total.png"
            else:
                outputFile_png_y = outputFile_png + "_Not_" + "_".join(TRAKDOCS) + "_Total.png"
            TextString = (
                "Database size at end : " + "{v:,.0f}".format(v=df_result.iloc[-1]["DatabaseUsedMB"] / 1024) + " GB"
            )
            generic_plot(
                df_result,
                "DatabaseUsedMB",
                ChartTitle,
                "MB",
                outputFile_png_y,
                False,
                True,
                TextString,
            )

            ChartTitle = (
                "Database Growth per Day" + includew + ", ".join(TRAKDOCS) + " " + RunDateStart + " to " + RunDateEnd
            )
            if INCLUDE:
                outputFile_png_y = outputFile_png + "_" + "_".join(TRAKDOCS) + "_Growth.png"
            else:
                outputFile_png_y = outputFile_png + "_Not_" + "_".join(TRAKDOCS) + "_Growth.png"
            TextString = (
                "Average database growth/day : "
                + "{v:,.3f}".format(v=DatabaseGrowthTotal / 1024 / df_result["DatabaseGrowthMB"].count())
                + " GB"
            )
            generic_plot(
                df_result,
                "DatabaseGrowthMB",
                ChartTitle,
                "MB",
                outputFile_png_y,
                False,
                False,
                TextString,
            )


def mainline(DIRECTORY, TRAKDOCS, Do_Globals):
    TITLEDATES = ""
    # Top N values. To do; make parameters
    TopNDatabaseByGrowth = 15
    TopNDatabaseByGrowthPie = 5
    TopNDatabaseByGrowthStack = 9

    colormapName = "Set1"
    # plt.style.use('seaborn-whitegrid')
    # plt.figure(num=None, figsize=(10, 6), dpi=300)
    # palette = plt.get_cmap(colormapName)
    # color=palette(1)
    # plt.plot(df_master['CPU'], color=color, alpha=0.7)
    # ax.grid(which='major', axis='both', linestyle='--')

    # Get list of files in directory, can have multiples of same type if follow regex
    MonitorAppName = glob.glob(DIRECTORY + "/*MonitorApp.txt")
    MonitorDatabaseName = glob.glob(DIRECTORY + "/*MonitorDatabase.txt")
    MonitorGlobalsName = glob.glob(DIRECTORY + "/*MonitorGlobals.txt")
    MonitorJournalsName = glob.glob(DIRECTORY + "/*MonitorJournals.txt")
    MonitorPageSummaryName = glob.glob(DIRECTORY + "/*MonitorPageSummary.txt")

    # Create directories for generated csv and png files
    if not os.path.exists(DIRECTORY + "/all_out_png"):
        os.mkdir(DIRECTORY + "/all_out_png")
    if not os.path.exists(DIRECTORY + "/all_out_csv"):
        os.mkdir(DIRECTORY + "/all_out_csv")
    if not os.path.exists(DIRECTORY + "/all_database"):
        os.mkdir(DIRECTORY + "/all_database")

    # Journals -------------------------------------------------------------------------
    # Total by day and output chart and processed data as csv

    for filename in MonitorJournalsName:
        outputName = os.path.splitext(os.path.basename(filename))[0]
        outputFile_png = DIRECTORY + "/all_out_png/" + outputName
        outputFile_csv = DIRECTORY + "/all_out_csv/" + outputName
        print("Journals: %s" % outputName)

        # Read in journal details, index on create date (column 3), sort on create date
        df_master = pd.read_csv(filename, sep="\t", encoding="ISO-8859-1", index_col=2, parse_dates=[2])
        df_master = df_master.dropna(axis=1, how="all")
        df_master.sort_index(inplace=True)

        # Remove all but last occurrences of duplicates, each day includes all inc previous days
        df_master = df_master[~df_master.index.duplicated(keep="last")]

        # Lets make display easier with a GB display
        df_master["Size GB"] = df_master["Size"] / (1024 * 1024 * 1024)

        # Beta - some seaborne Histograms - How are the journals distributed across the day?
        # Create some new columns to make display easier
        df_master["Create Date"] = df_master.index
        df_master["Create Hour"] = df_master["Create Date"].dt.hour
        df_master["Create Day"] = df_master["Create Date"].dt.day_name()
        df_master["Create Date"] = df_master["Create Date"].dt.date

        goBackDays = 8  # Could be smarter here, depends if collection ends am or pm
        cutoff_date = df_master["Create Date"].max() - pd.Timedelta(days=goBackDays)
        df_last_week = df_master[df_master["Create Date"] > cutoff_date]

        df_last_week.to_csv(outputFile_csv + "_Last_Week.csv", sep=",")

        # Start and end dates to display
        RunDateStart = df_last_week.head(1).index.strftime("%d/%m/%Y")
        RunDateEnd = df_last_week.tail(1).index.strftime("%d/%m/%Y")
        TITLEDATES = str(RunDateStart[0]) + " to " + str(RunDateEnd[0])

        plt.figure(figsize=(10, 6), dpi=300)
        plt.title("Journals switches across day  " + TITLEDATES, fontsize=14)
        plt.tick_params(labelsize=10)

        count_plot = sns.swarmplot(x="Create Day", y="Create Hour", data=df_last_week, hue="Reason", dodge=True)
        fig = count_plot.get_figure()
        fig.savefig(outputFile_png + "_swarm_plot.png")
        fig.clf()

        # Fun over, just usual chart....
        # Start and end dates to display
        RunDateStart = df_master.head(1).index.strftime("%d/%m/%Y")
        RunDateEnd = df_master.tail(1).index.strftime("%d/%m/%Y")
        TITLEDATES = str(RunDateStart[0]) + " to " + str(RunDateEnd[0])

        # Group per day
        df_day = df_master.groupby("Create Date").sum()

        df_day["Journal Sum GB"] = df_day["Size"] / (1024 * 1024 * 1024)
        df_day["Journal Sum GB"] = df_day["Journal Sum GB"].map("{:,.0f}".format).astype(int)

        TextString = "Average Journals/day : " + "{v:,.0f}".format(v=df_day["Journal Sum GB"].mean()) + " GB"
        TextString = TextString + ", Peak Journals/day : " + "{v:,.0f}".format(v=df_day["Journal Sum GB"].max()) + " GB"
        generic_plot(
            df_day,
            "Journal Sum GB",
            "Journals Per Day (GB)  " + TITLEDATES,
            "GB per Day",
            outputFile_png + "_per_day.png",
            False,
            True,
            TextString,
        )

        df_day.to_csv(outputFile_csv + "_by_Day.csv", sep=",")

    # Episodes  -------------------------------------------------------------------------
    # Output a few useful charts and convert input to csv

    for filename in MonitorAppName:
        outputName = os.path.splitext(os.path.basename(filename))[0]
        outputFile_png = DIRECTORY + "/all_out_png/" + outputName
        outputFile_csv = DIRECTORY + "/all_out_csv/" + outputName
        print("Episodes: %s" % outputName)

        df_master_ep = pd.read_csv(filename, sep="\t", encoding="ISO-8859-1", parse_dates=[0], index_col=0)
        df_master_ep = df_master_ep.dropna(axis=1, how="all")
        df_master_ep.index.names = ["Date"]
        df_master_ep.to_csv(outputFile_csv + ".csv", sep=",")

        RunDateStart = df_master_ep.head(1).index.tolist()
        RunDateStart = RunDateStart[0].strftime("%d/%m/%Y")
        RunDateEnd = df_master_ep.tail(1).index.tolist()
        RunDateEnd = RunDateEnd[0].strftime("%d/%m/%Y")
        TITLEDATES = RunDateStart + " to " + RunDateEnd

        TextString = "Average Episodes/day : " + "{v:,.0f}".format(v=df_master_ep["EpisodeCountTotal"].mean())
        TextString = (
            TextString + ", Peak Episodes/day : " + "{v:,.0f}".format(v=df_master_ep["EpisodeCountTotal"].max())
        )
        TextString = (
            TextString + ", Est Episodes/year : " + "{v:,.0f}".format(v=df_master_ep["EpisodeCountTotal"].mean() * 365)
        )
        TextString = (
            TextString + "\nPeak Episodes/hour : " + "{v:,.0f}".format(v=df_master_ep["EpisodePeakPerHourCount"].max())
        )
        TextString = (
            TextString + ", Peak Episodes/min : " + "{v:,.0f}".format(v=df_master_ep["EpisodePeakPerMinuteCount"].max())
        )

        generic_plot(
            df_master_ep,
            "EpisodeCountTotal",
            "Total Episodes Per Day  " + TITLEDATES,
            "Episodes per Day",
            outputFile_png + "_Ttl_Episodes.png",
            False,
            True,
            TextString,
        )
        generic_plot(
            df_master_ep,
            "OrderCountTotal",
            "Total Orders Per Day  " + TITLEDATES,
            "Orders per Day",
            outputFile_png + "_Ttl_Orders.png",
            False,
            True,
        )

        # Example of multiple charts. To do; Make this a function to accept any number of items

        plt.style.use("seaborn-whitegrid")
        plt.figure(num=None, figsize=(10, 6), dpi=300)
        palette = plt.get_cmap(colormapName)
        color = palette(1)

        plt.plot(df_master_ep["EpisodeCountTotal"], label="Total Episodes Per Day")
        plt.plot(df_master_ep["OrderCountTotal"], label="Total Orders Per Day")
        plt.legend(loc="best")

        plt.title("Episodes and Orders by Day  " + TITLEDATES, fontsize=14)
        plt.ylabel("Count", fontsize=10)
        plt.tick_params(labelsize=10)
        ax = plt.gca()
        ax.set_ylim(bottom=0)  # Always zero start
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))
        ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.WeekdayLocator(byweekday=MO)))
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(outputFile_png + "_Ttl_Episodes_Orders.png", format="png")
        plt.close()

        plt.style.use("seaborn-whitegrid")
        plt.figure(num=None, figsize=(10, 6), dpi=300)
        palette = plt.get_cmap(colormapName)
        color = palette(1)

        # What are the busiest days?
        df_master_ep["Day"] = df_master_ep.index.to_series().dt.day_name()

        plt.title("Episodes by Day " + TITLEDATES, fontsize=14)
        plt.tick_params(labelsize=10)

        count_plot = sns.swarmplot(x="Day", y="EpisodeCountTotal", data=df_master_ep)
        count_plot.set(ylabel="Count", xlabel="")
        count_plot.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))

        fig = count_plot.get_figure()
        fig.savefig(outputFile_png + "_swarm_plot.png")

    # Databases  -------------------------------------------------------------------------
    # Total by day and output full list, by day list, top n growth and chart top n growth

    for filename in MonitorDatabaseName:

        outputName = os.path.splitext(os.path.basename(filename))[0]
        outputFile_png = DIRECTORY + "/all_out_png/" + outputName + "_Summary"
        outputFile_csv = DIRECTORY + "/all_out_csv/" + outputName + "_Summary"
        print("Databases: %s" % outputName)

        # What is the total size of all databases? includes CACHETEMP

        df_master_db = pd.read_csv(filename, sep="\t", encoding="ISO-8859-1", parse_dates=[0], index_col=0)
        df_master_db = df_master_db.dropna(axis=1, how="all")

        df_master_db.index.names = ["Date"]
        df_master_db["DatabaseUsedMB"] = df_master_db["SizeinMB"] - df_master_db["FreeSpace"]

        df_db_by_date = df_master_db.groupby("Date").sum()

        df_master_db.to_csv(outputFile_csv + "_Size.csv", sep=",")
        df_db_by_date.to_csv(outputFile_csv + "_Size_by_date.csv", sep=",")

        # Data growth
        TextString = (
            "Database size used at end : "
            + "{v:,.0f}".format(v=df_db_by_date.iloc[-1]["DatabaseUsedMB"] / 1024)
            + " GB (includes CACHETEMP)\n"
        )
        generic_plot(
            df_db_by_date,
            "DatabaseUsedMB",
            "Total Database Used  " + TITLEDATES,
            "(MB)",
            outputFile_png + "_Ttl_Database_Used.png",
            False,
            True,
            TextString,
        )

        # Actual usage on disk
        TextString = (
            "Database size on disk (inc Freespace) at end : "
            + "{v:,.0f}".format(v=df_db_by_date.iloc[-1]["SizeinMB"] / 1024)
            + " GB (includes CACHETEMP)\n"
        )
        generic_plot(
            df_db_by_date,
            "SizeinMB",
            "Total Database Size on Disk  " + TITLEDATES,
            "(MB)",
            outputFile_png + "_Ttl_Database_Size_On_Disk.png",
            False,
            True,
            TextString,
        )

        TextString = (
            "Database free at end : "
            + "{v:,.0f}".format(v=df_db_by_date.iloc[-1]["FreeSpace"] / 1024)
            + " GB (includes CACHETEMP)\n"
        )
        generic_plot(
            df_db_by_date,
            "FreeSpace",
            "Total Database Freespace on Disk  " + TITLEDATES,
            "(MB)",
            outputFile_png + "_Ttl_Database_Free.png",
            False,
            True,
            TextString,
        )

        # What are the high growth databases in this period?
        # Get database sizes, dont key by date as we will use this field

        df_master_db = pd.read_csv(filename, sep="\t", encoding="ISO-8859-1")
        df_master_db = df_master_db.dropna(axis=1, how="all")
        df_master_db = df_master_db.rename(columns={"RunDate": "Date"})
        df_master_db["DatabaseUsedMB"] = df_master_db["SizeinMB"] - df_master_db["FreeSpace"]

        # create a new file per database for later deep dive if needed
        df_databases = pd.DataFrame({"Name": df_master_db.Name.unique()})  # Get unique database names

        cols = ["Database", "Start MB", "End MB", "Growth MB"]
        lst = []
        for index, row in df_databases.iterrows():
            df_temp = df_master_db.loc[df_master_db["Name"] == row["Name"]].iloc[[0, -1]]
            lst.append(
                [
                    row["Name"],
                    df_temp["DatabaseUsedMB"].iloc[0],
                    df_temp["DatabaseUsedMB"].iloc[1],
                    df_temp["DatabaseUsedMB"].iloc[1] - df_temp["DatabaseUsedMB"].iloc[0],
                ]
            )
            df_master_db.loc[df_master_db["Name"] == row["Name"]].to_csv(
                DIRECTORY + "/all_database/Database_" + row["Name"] + ".csv",
                sep=",",
                index=False,
            )

        # Lets see growth over sample period in some charts
        df_databases_by_growth = pd.DataFrame(lst, columns=cols).sort_values(by=["Growth MB"], ascending=False)
        df_databases_by_growth.to_csv(outputFile_csv + ".csv", sep=",", index=False)

        # What are the top N databses by growth? df_databases_by_growth will hold the sorted list
        df_databases_by_growth.head(TopNDatabaseByGrowth).to_csv(
            outputFile_csv + "_top_" + str(TopNDatabaseByGrowth) + ".csv",
            sep=",",
            index=False,
        )

        # Bar chart - top N Total Growth
        plt.style.use("seaborn-whitegrid")
        plt.figure(num=None, figsize=(10, 6), dpi=300)
        palette = plt.get_cmap(colormapName)
        color = palette(1)
        index = np.arange(len(df_databases_by_growth["Database"].head(TopNDatabaseByGrowth)))

        plt.barh(
            df_databases_by_growth["Database"].head(TopNDatabaseByGrowth),
            df_databases_by_growth["Growth MB"].head(TopNDatabaseByGrowth),
        )

        plt.title(
            "Top " + str(TopNDatabaseByGrowth) + " - Database Growth  " + TITLEDATES,
            fontsize=14,
        )
        plt.xlabel("Growth over period (MB)", fontsize=10)
        plt.tick_params(labelsize=10)
        plt.yticks(
            index,
            df_databases_by_growth["Database"].head(TopNDatabaseByGrowth),
            fontsize=10,
        )
        ax = plt.gca()
        ax.xaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))
        plt.tight_layout()
        plt.savefig(
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowth) + "_Bar.png",
            format="png",
        )
        plt.close()

        # Growth of top n databases over time (not stacked)
        df_master_db["Date"] = pd.to_datetime(df_master_db["Date"])  # Convert text field to date time

        top_List = df_databases_by_growth["Database"].head(TopNDatabaseByGrowthStack).tolist()
        grpd = df_master_db.groupby("Name")

        plt.style.use("seaborn-whitegrid")
        plt.figure(num=None, figsize=(10, 6), dpi=300)
        palette = plt.get_cmap(colormapName)
        color = palette(1)

        for name, data in grpd:
            if name in top_List:
                plt.plot(data.Date.values, data.DatabaseUsedMB.values, "-", label=name)

        plt.title("Top Growth Databases (Not Stacked)  " + TITLEDATES, fontsize=14)
        plt.ylabel("MB", fontsize=10)
        plt.tick_params(labelsize=10)
        plt.legend(loc="upper left")
        ax = plt.gca()
        ax.set_ylim(bottom=0)  # Always zero start
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))
        ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.WeekdayLocator(byweekday=MO)))
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowthStack) + "_Growth_Time.png",
            format="png",
        )
        plt.close()

        # Pie chart to show relative sizes, First and Last day of sample period
        FirstDay = df_master_db["Date"].iloc[0]
        df_temp = df_master_db.loc[df_master_db["Date"] == FirstDay]

        df_sorted = df_temp.sort_values(by=["DatabaseUsedMB"], ascending=False)
        df_sorted.to_csv(outputFile_csv + "_pie.csv", sep=",", index=False)

        # Drop rows with unmounted databases - size shows up as NaN
        # df_sorted = df_sorted.dropna() <--- cant use this drops too much

        Total_all_db = df_sorted["DatabaseUsedMB"].sum()
        TOTAL_ALL_DB = Total_all_db / 1024

        df_sorted["Labels"] = np.where(df_sorted["DatabaseUsedMB"] * 100 / Total_all_db > 2, df_sorted["Name"], "")

        plt.style.use("seaborn-whitegrid")

        current_palette_10 = sns.color_palette("Paired", 10)
        sns.set_palette(current_palette_10)

        plt.figure(num=None, figsize=(10, 6), dpi=300)
        pie_exp = tuple(0.1 if i < 2 else 0 for i in range(df_sorted["Name"].count()))  # Pie explode

        plt.pie(
            df_sorted["DatabaseUsedMB"],
            labels=df_sorted["Labels"],
            autopct=make_autopct(df_sorted["DatabaseUsedMB"]),
            startangle=60,
            explode=pie_exp,
            shadow=True,
        )
        plt.title(
            "Top Database Sizes at Start " + str(FirstDay) + " - Total " + "{v:,.0f}".format(v=TOTAL_ALL_DB) + " GB",
            fontsize=14,
        )

        plt.axis("equal")
        plt.tight_layout()
        plt.savefig(outputFile_png + "_Total_DB_Size_Pie_Start.png")
        plt.close()

        # Last day of sample period
        LastDay = df_master_db["Date"].iloc[-1]
        df_temp = df_master_db.loc[df_master_db["Date"] == LastDay]

        df_sorted = df_temp.sort_values(by=["DatabaseUsedMB"], ascending=False)
        df_sorted.to_csv(outputFile_csv + "_pie.csv", sep=",", index=False)

        # Drop rows with unmounted databases - size shows up as NaN
        # df_sorted = df_sorted.dropna() <--- cant use this drops too much

        Total_all_db = df_sorted["DatabaseUsedMB"].sum()
        TOTAL_ALL_DB = Total_all_db / 1024

        df_sorted["Labels"] = np.where(df_sorted["DatabaseUsedMB"] * 100 / Total_all_db > 2, df_sorted["Name"], "")

        plt.style.use("seaborn-whitegrid")
        plt.figure(num=None, figsize=(10, 6), dpi=300)
        pie_exp = tuple(0.1 if i < 2 else 0 for i in range(df_sorted["Name"].count()))  # Pie explode

        plt.pie(
            df_sorted["DatabaseUsedMB"],
            labels=df_sorted["Labels"],
            autopct=make_autopct(df_sorted["DatabaseUsedMB"]),
            startangle=60,
            explode=pie_exp,
            shadow=True,
        )
        plt.title(
            "Top Database Sizes at " + str(LastDay) + " - Total " + "{v:,.0f}".format(v=TOTAL_ALL_DB) + " GB",
            fontsize=14,
        )

        plt.axis("equal")
        plt.tight_layout()
        plt.savefig(outputFile_png + "_Total_DB_Size_Pie_End.png")
        plt.close()

        # Stacked Chart is a good way to look at Top N- this was more painful than I expected, but hey, its to hot to go outside.

        # Because a stacked chart is built with python lists if the value list does not have data for a date
        # (eg DB did not exist on a date) stackplot will fail because there is not a value for each xaxis date.
        # Also the lists will get out of synch.
        # So need to substitue zero for all items for date when there is no data (eg the db did not exist)
        # an example is a newly created audit database

        # Start by getting a list of top database names by growth, and a dataframe with just them in it

        top_List = df_databases_by_growth["Database"].head(TopNDatabaseByGrowthStack).tolist()
        df_top_List = df_master_db[df_master_db["Name"].isin(top_List)]

        # We only care about a few columns
        df_top_List = df_top_List.filter(["Date", "Name", "DatabaseUsedMB"], axis=1)
        # Lets make the lookup easier
        df_top_List["Date_Name"] = df_top_List["Date"].map(str) + df_top_List["Name"]

        # Get unique dates in list
        dates = pd.DataFrame({"Date": df_top_List.Date.unique()})
        dates = dates["Date"].tolist()

        # If there are databases missing for a date create a 0 row
        # Create new rows in lists first (appending each one individually to df is slow)
        Date_append_list = []
        Name_append_list = []
        DatabaseUsedMB_append_list = []
        Date_Name_append_list = []

        for unique_date in dates:
            for name in top_List:
                # If not found create row
                if ~df_top_List["Date_Name"].str.contains(str(unique_date) + name).any():
                    Date_append_list.append(unique_date)
                    Name_append_list.append(name)
                    DatabaseUsedMB_append_list.append(0)
                    Date_Name_append_list.append(str(unique_date) + name)

        df_top_List = df_top_List.append(
            pd.DataFrame(
                {
                    "Date": Date_append_list,
                    "Name": Name_append_list,
                    "DatabaseUsedMB": DatabaseUsedMB_append_list,
                    "Date_Name": Date_Name_append_list,
                }
            ),
            sort=False,
        )

        # Sort the dataframe, won't use an index
        df_top_List.sort_values(by=["Date", "Name"], inplace=True)

        # Now finish building the lists for the stackplot

        # {Dictionary} to hold top N database Names and sizes over time
        # {'TRAK_MONITOR': [1930, 4000, 7376, 10886, 14263, etc....], 'TRAK_DOCS': [924247,....]}
        Lists = {}

        # Create a list for each database in the top list
        for i in top_List:
            df_A = df_top_List[df_top_List["Name"] == i]
            listName = i.replace("-", "_")  # Dashes screw with Python
            Lists[listName] = df_A["DatabaseUsedMB"].tolist()

        # Build new list of values from dictionary
        all_keys = []
        all_values = []
        for i, j in Lists.items():
            all_keys.append(i)
            all_values.append(j)

        plt.style.use("seaborn-whitegrid")
        plt.figure(num=None, figsize=(10, 6), dpi=300)

        palette = plt.get_cmap(colormapName)
        palette_cycle = sns.color_palette("Set1")

        # print(f"{dates}\n{all_values}\n{all_keys}")
        # This next plot can fail because of mismatch dates and values
        plt.stackplot(dates, all_values, labels=all_keys, colors=palette_cycle, alpha=0.5)

        plt.title(
            "Top " + str(TopNDatabaseByGrowthStack) + " - Database Growth  " + TITLEDATES,
            fontsize=14,
        )
        plt.ylabel("MB", fontsize=10)
        plt.tick_params(labelsize=10)
        ax = plt.gca()
        ax.grid(which="major", axis="both", linestyle="--")
        ax.set_ylim(bottom=0)  # Always zero start
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))
        ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.WeekdayLocator(byweekday=MO)))
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        plt.tight_layout()
        plt.legend(loc="upper left")

        plt.savefig(
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowthStack) + "_Growth_Time_Stack.png",
            format="png",
        )
        plt.close()

        df_top_List.to_csv(outputFile_csv + "_top_list.csv", sep=",", index=False)

    # Average Episode size is good to know  - Merge Episodes and Database growth (grouped by date)

    for index in range(len(MonitorAppName)):

        # Now plot the data

        average_episode_size(DIRECTORY, MonitorAppName[index], MonitorDatabaseName[index], ["all"], True)

        if TRAKDOCS == [""]:
            print(
                'TrakCare document database not defined - use -t "TRAK-DOCDBNAME" to calculate growth with/without docs'
            )
        else:
            if len(TRAKDOCS) > 1:
                for options in TRAKDOCS:
                    average_episode_size(
                        DIRECTORY,
                        MonitorAppName[index],
                        MonitorDatabaseName[index],
                        [options],
                        True,
                    )
                    average_episode_size(
                        DIRECTORY,
                        MonitorAppName[index],
                        MonitorDatabaseName[index],
                        [options],
                        False,
                    )

            average_episode_size(
                DIRECTORY,
                MonitorAppName[index],
                MonitorDatabaseName[index],
                TRAKDOCS,
                True,
            )
            average_episode_size(
                DIRECTORY,
                MonitorAppName[index],
                MonitorDatabaseName[index],
                TRAKDOCS,
                False,
            )

    # Globals - takes a while, explicitly run it without -g option -------------------------

    if not Do_Globals:

        for filename in MonitorGlobalsName:

            if not os.path.exists(DIRECTORY + "/all_globals"):
                os.mkdir(DIRECTORY + "/all_globals")

            outputName = os.path.splitext(os.path.basename(filename))[0]
            outputFile_png = DIRECTORY + "/all_out_png/" + outputName + "_Summary"
            outputFile_csv = DIRECTORY + "/all_out_csv/" + outputName + "_Summary"

            print("Globals: %s" % outputName)

            df_master_gb = pd.read_csv(filename, sep="\t", encoding="ISO-8859-1")
            df_master_gb = df_master_gb.dropna(axis=1, how="all")
            df_master_gb = df_master_gb.rename(columns={"RunDate": "Date"})

            # substring mapping is a thing - one global can have many parts, need to break on path and Global
            #  DataBasePath	        GlobalName	SizeAllocated
            # /db/AUDIT0/	AUD	    57949
            # /db/AUDIT1/	AUD	    103617
            # /db/AUDIT2/	AUD	    45235
            # /db/AUDIT3/	AUD	    41815
            # etc

            df_master_gb["DataBasePath"].replace("\\\\", "_", inplace=True, regex=True)
            df_master_gb["DataBasePath"].replace(":", "_", inplace=True, regex=True)
            df_master_gb["DataBasePath"].replace("/", "_", inplace=True, regex=True)
            df_master_gb["DataBasePath"].replace("__", "", inplace=True, regex=True)

            # Add full path name, Size recalculated in GB
            df_master_gb["Full_Global"] = df_master_gb["DataBasePath"].str[1:] + df_master_gb["GlobalName"]
            df_master_gb["SizeAllocatedGB"] = df_master_gb["SizeAllocated"] / 1024

            # Get unique names and use that as a key to create a new dataframe per global
            df_globals = pd.DataFrame({"Full_Global": df_master_gb.Full_Global.unique()})  # Get unique names

            # Sort the dataframe, won't use an index - just to be sure it stil in date order
            df_master_gb.sort_values(by=["Date", "Full_Global"], inplace=True)

            # Create a summary dataframe that can be sorted
            cols = ["Full_Global", "Start Size", "End Size", "Growth Size"]
            lst = []

            print("Please wait while globals growth calculated, this may take a while -- there a lot of them")
            dots = "."

            # Iterate over all the rows in the dataframe. Returns index and the whole row.
            for index, row in df_globals.iterrows():

                # For each row create a temporary dataframe with just that globals first and last rows.
                df_temp = df_master_gb.loc[df_master_gb["Full_Global"] == row["Full_Global"]].iloc[[0, -1]]

                # Create a list with full name, size at start, size at end (only 2 rows in df (0 and 1)), and difference (growth)
                lst.append(
                    [
                        row["Full_Global"],
                        df_temp["SizeAllocated"].iloc[0],
                        df_temp["SizeAllocated"].iloc[1],
                        df_temp["SizeAllocated"].iloc[1] - df_temp["SizeAllocated"].iloc[0],
                    ]
                )

                # Something to look at
                dots += "."
                if dots == ".........................................................":
                    dots = "."
                print("\r" + dots, end="")
            print("\n")

            # Create a dataframe with just the rows and columns we care about
            df_globals_by_growth = pd.DataFrame(lst, columns=cols).sort_values(by=["Growth Size"], ascending=False)
            df_globals_by_growth.to_csv(outputFile_csv + ".csv", sep=",", index=False)

            df_globals_by_growth.head(TopNDatabaseByGrowth).to_csv(
                outputFile_csv + "_top_" + str(TopNDatabaseByGrowth) + ".csv",
                sep=",",
                index=False,
            )

            # Get a list of the top N globals
            top_List = df_globals_by_growth["Full_Global"].head(TopNDatabaseByGrowth).tolist()

            # Lets see the highest growth globals - bar chart

            plt.style.use("seaborn-whitegrid")
            current_palette_10 = sns.color_palette("Paired", TopNDatabaseByGrowth)
            sns.set_palette(current_palette_10)

            plt.figure(num=None, figsize=(10, 6), dpi=300)
            index = np.arange(len(df_globals_by_growth["Full_Global"].head(TopNDatabaseByGrowth)))
            plt.barh(
                df_globals_by_growth["Full_Global"].head(TopNDatabaseByGrowth),
                df_globals_by_growth["Growth Size"].head(TopNDatabaseByGrowth),
            )

            plt.title(
                "Top " + str(TopNDatabaseByGrowth) + " - Globals by Growth  " + TITLEDATES,
                fontsize=14,
            )
            plt.xlabel("Growth over period (MB)", fontsize=10)
            plt.tick_params(labelsize=10)
            plt.yticks(
                index,
                df_globals_by_growth["Full_Global"].head(TopNDatabaseByGrowth),
                fontsize=10,
            )
            ax = plt.gca()
            ax.xaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))
            plt.tight_layout()
            plt.savefig(
                outputFile_png + "_Top_" + str(TopNDatabaseByGrowth) + ".png",
                format="png",
            )
            plt.close()

            # Growth of top n globals - Not Stacked
            df_master_gb["Date"] = pd.to_datetime(df_master_gb["Date"])

            grpd = df_master_gb.groupby("Full_Global")

            plt.style.use("seaborn-whitegrid")
            current_palette_10 = sns.color_palette("Paired", TopNDatabaseByGrowth)
            sns.set_palette(current_palette_10)

            plt.figure(num=None, figsize=(10, 6), dpi=300)

            for name, data in grpd:
                if name in top_List:
                    plt.plot(data.Date.values, data.SizeAllocatedGB.values, "-", label=name)
            plt.legend(loc="best")

            plt.title("Top Growth Globals Over Period  " + TITLEDATES, fontsize=14)
            plt.ylabel("GB", fontsize=10)
            plt.tick_params(labelsize=10)
            ax = plt.gca()
            ax.set_ylim(bottom=0)  # Always zero start
            ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))
            ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.WeekdayLocator(byweekday=MO)))
            plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
            plt.tight_layout()
            plt.savefig(
                outputFile_png + "_Top_" + str(TopNDatabaseByGrowth) + "_Growth.png",
                format="png",
            )
            plt.close()

            # Print the full history of the top N globals
            for full_name in top_List:
                df_master_gb.loc[df_master_gb["Full_Global"] == full_name].to_csv(
                    DIRECTORY + "/all_globals/Globals_" + full_name + ".csv",
                    sep=",",
                    index=False,
                )

            # Set date index for individual plots
            df_master_gb.set_index("Date", inplace=True)

            x = 0
            for full_name in top_List:
                df_gb_top_ind = df_master_gb[df_master_gb.Full_Global == full_name]

                TextString = (
                    "Global size on disk at end : "
                    + "{v:,.0f}".format(v=df_gb_top_ind.iloc[-1]["SizeAllocatedGB"])
                    + " GB "
                    + full_name
                )
                generic_plot(
                    df_gb_top_ind,
                    "SizeAllocatedGB",
                    "Total Global Size on Disk _" + TITLEDATES,
                    "(GB)",
                    outputFile_png + "_" + str(x) + "_Ttl_Global_Size_On_Disk" + full_name + ".png",
                    False,
                    True,
                    TextString,
                )
                x = x + 1

            # PIE chart of total global size
            # --------------------------------

            # Sort the summary dataframe by End Size
            df_sorted = df_globals_by_growth.sort_values(by=["End Size"], ascending=False)
            df_sorted.to_csv(outputFile_csv + "_pie.csv", sep=",", index=False)

            Total_all_gb = df_sorted["End Size"].sum()

            df_sorted["Labels"] = np.where(
                df_sorted["End Size"] * 100 / Total_all_gb > 2,
                df_sorted["Full_Global"],
                "",
            )

            plt.style.use("seaborn-whitegrid")
            current_palette_10 = sns.color_palette("Paired", 12)
            sns.set_palette(current_palette_10)
            plt.figure(num=None, figsize=(10, 6), dpi=300)

            pie_exp = tuple(0.1 if i < 2 else 0 for i in range(df_sorted["Full_Global"].count()))  # Pie explode

            plt.pie(
                df_sorted["End Size"],
                labels=df_sorted["Labels"],
                autopct=make_autopct(df_sorted["End Size"]),
                startangle=60,
                explode=pie_exp,
                shadow=True,
            )
            plt.title(
                "Top Global Sizes at " + str(LastDay) + " - Total " + "{v:,.0f}".format(v=Total_all_gb / 1024) + " GB",
                fontsize=14,
            )

            plt.axis("equal")
            plt.tight_layout()
            plt.savefig(outputFile_png + "_Total_global_Size_Pie_End.png")
            plt.close()

    # Page Summary

    for filename in MonitorPageSummaryName:

        if not os.path.exists(DIRECTORY + "/all_pages"):
            os.mkdir(DIRECTORY + "/all_pages")

        outputName = os.path.splitext(os.path.basename(filename))[0]
        outputFile_png = DIRECTORY + "/all_out_png/" + outputName + "_Summary"
        outputFile_csv = DIRECTORY + "/all_out_csv/" + outputName + "_Summary"

        print("Page Summary: %s" % outputName)

        # What are the high growth pages in this period?
        # Get glorefs, dont key by date as we will use this field

        df_master_ps = pd.read_csv(filename, sep="\t", encoding="ISO-8859-1")
        df_master_ps = df_master_ps.dropna(axis=1, how="all")
        df_master_ps = df_master_ps.rename(columns={"RunDate": "Date"})

        # Time does not seem to be exported properly
        # mask = df_master_ps.SumPTime >0
        # df_master_ps.loc[mask, "AvgPTime"] = df_master_ps["SumPTime"] / df_master_ps["TotalHits"]
        df_master_ps["AvgPTime"] = df_master_ps["SumPTime"] / df_master_ps["TotalHits"]
        df_master_ps.to_csv(outputFile_csv + "_df_master_ps.csv", sep=",")

        # Group by name Hits
        df_ps_by_TotalHits = df_master_ps.groupby(["pName"], sort=True).sum().reset_index()
        df_ps_by_TotalHits = df_ps_by_TotalHits.sort_values(by=["TotalHits"], ascending=[False])
        df_ps_by_TotalHits.to_csv(outputFile_csv + "_Name_TotalHits.csv", sep=",")

        # Group by name SumPGlobals
        df_ps_by_SumPGlobals = df_master_ps.groupby(["pName"], sort=True).sum().reset_index()
        df_ps_by_SumPGlobals = df_ps_by_SumPGlobals.sort_values(by=["SumPGlobals"], ascending=[False])
        df_ps_by_SumPGlobals.to_csv(outputFile_csv + "_Name_SumPGlobals.csv", sep=",")

        # Group by name AvgPGlobals
        df_ps_by_AvgPGlobals = df_master_ps.groupby(["pName"], sort=True).sum().reset_index()
        df_ps_by_AvgPGlobals = df_ps_by_AvgPGlobals.sort_values(by=["AvgPGlobals"], ascending=[False])
        df_ps_by_AvgPGlobals.to_csv(outputFile_csv + "_Name_AvgPGlobals.csv", sep=",")

        # Group by name MaxPGlobals
        df_ps_by_MaxPGlobals = df_master_ps.groupby(["pName"], sort=True).sum().reset_index()
        df_ps_by_MaxPGlobals = df_ps_by_MaxPGlobals.sort_values(by=["MaxPGlobals"], ascending=[False])
        df_ps_by_MaxPGlobals.to_csv(outputFile_csv + "_Name_MaxPGlobals.csv", sep=",")

        # Group by name SumPTime
        df_ps_by_SumPTime = df_master_ps.groupby(["pName"], sort=True).sum().reset_index()
        df_ps_by_SumPTime = df_ps_by_SumPTime.sort_values(by=["SumPTime"], ascending=[False])
        df_ps_by_SumPTime.to_csv(outputFile_csv + "_Name_SumPTime.csv", sep=",")

        # Plot the top N by ....
        df_master_ps["Date"] = pd.to_datetime(df_master_ps["Date"])

        generic_top_n(
            df_ps_by_SumPGlobals,
            TopNDatabaseByGrowthStack,
            df_master_ps,
            "SumPGlobals",
            "High Sum Globals (Not Stacked)  " + TITLEDATES,
            "Sum Globals",
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowthStack) + "_Sum_Globals.png",
            pres=False,
        )

        generic_top_n(
            df_ps_by_AvgPGlobals,
            TopNDatabaseByGrowthStack,
            df_master_ps,
            "AvgPGlobals",
            "High Average Globals (Not Stacked)  " + TITLEDATES,
            "Average Globals",
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowthStack) + "_Average_Globals.png",
            pres=False,
        )

        generic_top_n(
            df_ps_by_SumPTime,
            TopNDatabaseByGrowthStack,
            df_master_ps,
            "SumPTime",
            "High Sum Time (Not Stacked)  " + TITLEDATES,
            "Sum Time",
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowthStack) + "_SumPTime.png",
            pres=False,
        )

        generic_top_n(
            df_ps_by_TotalHits,
            TopNDatabaseByGrowthStack,
            df_master_ps,
            "TotalHits",
            "High Hits (Not Stacked)  " + TITLEDATES,
            "Number of Hits",
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowthStack) + "_TotalHits.png",
            pres=False,
        )

        # Note top 10
        generic_top_n(
            df_ps_by_TotalHits,
            TopNDatabaseByGrowth,
            df_master_ps,
            "SumPGlobals",
            "Sum Globals for High Hits (Not Stacked)  " + TITLEDATES,
            "Sum Globals",
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowth) + "_TotalHits_SumGlobals.png",
            pres=False,
        )

        generic_top_n(
            df_ps_by_MaxPGlobals,
            TopNDatabaseByGrowth,
            df_master_ps,
            "AvgPGlobals",
            "High Maximum Average Globals (Not Stacked)  " + TITLEDATES,
            "Average Globals",
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowth) + "_MaxPGlobals.png",
            pres=False,
        )

        generic_top_n(
            df_ps_by_TotalHits,
            TopNDatabaseByGrowth,
            df_master_ps,
            "AvgPGlobals",
            "Average Globals for High Hits (Not Stacked)  " + TITLEDATES,
            "Average Globals",
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowth) + "_TotalHits_AvgPGlobals.png",
            pres=False,
        )

        generic_top_n(
            df_ps_by_TotalHits,
            TopNDatabaseByGrowth,
            df_master_ps,
            "SumPTime",
            "Sum Time for High Hits (Not Stacked)  " + TITLEDATES,
            "Sum Time",
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowth) + "_TotalHits_SumPTime.png",
            pres=False,
        )

        generic_top_n(
            df_ps_by_SumPGlobals,
            TopNDatabaseByGrowth,
            df_master_ps,
            "AvgPGlobals",
            "Average Globals for High Sum Globals (Not Stacked)  " + TITLEDATES,
            "Average Globals",
            outputFile_png + "_Top_" + str(TopNDatabaseByGrowth) + "_SumPGlobals_AvgPGlobals.png",
            pres=False,
        )

        # Set date index for individual plots
        df_master_ps.set_index("Date", inplace=True)

        # get top by sum globals and display charts
        top_List = df_ps_by_SumPGlobals["pName"].head(TopNDatabaseByGrowth).tolist()

        plt.style.use("seaborn-whitegrid")
        plt.figure(figsize=(10, 6), dpi=300)

        x = 0
        for name in top_List:
            df_ps_top_ind = df_master_ps[df_master_ps.pName == name]
            fig, ax1 = plt.subplots()
            plt.gcf().set_size_inches(10, 6)
            plt.gcf().set_dpi(300)
            color = "g"
            line1 = ax1.plot(df_ps_top_ind["AvgPGlobals"], color=color)
            ax1.set_title(
                "Average Globals and Time by day " + TITLEDATES + "\n" + name,
                fontsize=14,
            )
            ax1.set_ylabel("Average Globals", fontsize=10, color=color)
            ax1.tick_params(labelsize=10)
            ax1.set_ylim(bottom=0)  # Always zero start
            ax1.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))
            ax1.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.WeekdayLocator(byweekday=MO)))

            ax2 = ax1.twinx()
            color = "b"
            line2 = ax2.plot(df_ps_top_ind["AvgPTime"], color=color)
            ax2.set_ylabel("Average Time", fontsize=10, color=color)
            ax2.tick_params(labelsize=10)
            ax2.set_ylim(bottom=0)  # Always zero start
            ax2.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.2f}"))
            ax2.grid(None)
            # plot_text_string = ""
            # fig.text(0.01,.95,plot_text_string, ha='left', va='center', transform=ax.transAxes, fontsize=12)

            # added these three lines to get all labels in one legend
            # lines = line1 + line2
            # labels = [l.get_label() for l in lines]
            # ax1.legend(lines, labels, loc="best")

            plt.setp(ax1.get_xticklabels(), rotation=45, ha="right")
            plt.tight_layout()
            plt.savefig(
                outputFile_png + "_" + str(x) + "_" + name + "_Globals_Time.png",
                format="png",
            )
            plt.close(fig)
            x = x + 1

    print("Finished\n")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="tc_monitor_unpack", description="TrakCare Monitor Process", epilog='Be safe, "quote the path"'
    )
    parser.add_argument("-d", "--directory", help="Directory with Monitor files", required=True, metavar='"/path/path"')
    parser.add_argument(
        "-l",
        "--listofDBs",
        nargs="+",
        help="TrakCare databases names to show separately for average episode size",
    )
    parser.add_argument(
        "-g", "--exclude_globals", help="Globals metrics take a long time and can be excluded", action="store_true"
    )
    # parser.add_argument("-p", "--page", help="Page Summary take a long time", action="store_true")

    args = parser.parse_args()

    if args.directory is not None:
        DIRECTORY = args.directory
        try:
            if os.path.getsize(args.directory) > 0:
                input_file = args.directory
            else:
                print('Error: -d "Directory with Monitor files"')
                sys.exit()
        except OSError as e:
            print("Could not process files because: {}".format(str(e)))
            sys.exit()
    else:
        print('Error: -d "Directory with Monitor files"')
        sys.exit()

    if args.listofDBs is not None:
        TRAKDOCS = args.listofDBs
    else:
        TRAKDOCS = [""]

    try:
        mainline(DIRECTORY, TRAKDOCS, args.exclude_globals)
    except OSError as e:
        print("Could not process files because: {}".format(str(e)))
