"""
The files are in a variety of formats. This script will convert all to pdf.
"""
from glob import glob
import shutil

import pandas as pd
import tabula
import odf

def main():
    """
    Convert all files to csv files, saved in directory csvfiles.
    """
    dirname = "csvfiles"

    # old excel files
    for filename in glob("*xls"):
        print(filename)
        df = pd.read_excel(filename, sheet_name=None)
        for i, d in enumerate(df.values(), 1):
            outfile = f"{dirname}/{i}_" + filename.replace("xls", "csv")
            print(outfile)
            d.to_csv(outfile)

    # modern excel files
    for filename in glob("*xlsx"):
        print(filename)
        df = pd.read_excel(filename, sheet_name=None)
        for i, d in enumerate(df.values(), 1):
            outfile = f"{dirname}/{i}_" + filename.replace("xlsx", "csv") 
            print(outfile)
            d.to_csv(outfile)

    # open office files
    for filename in glob("*ods"):
        print(filename)
        df = pd.read_excel(filename, sheet_name=None, engine="odf")
        for i, d in enumerate(df.values(), 1):
            outfile = f"{dirname}/{i}_" + filename.replace("ods", "csv")
            print(outfile)
            d.to_csv(outfile)

    # pdf files, 1 table per file
    for filename in glob("*pdf"):
        print(filename)
        df = tabula.read_pdf(filename, pages=1)[0]
        outfile = f"{dirname}/1_" + filename.replace("pdf", "csv")
        print(outfile)
        df.to_csv(f"{dirname}/1_" + filename.replace("pdf", "csv"))

    # copy csv files
    for filename in glob("*csv"):
        print(filename)
        shutil.copyfile(filename, f"1_{dirname}/{filename}")

if __name__ == "__main__":
    main()
