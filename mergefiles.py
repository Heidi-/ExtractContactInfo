"""
Now that all the files are csv, and the headers have been manually edited where they were lacking,
time to combine all the files. Cases to deal with:

    - Header casing is inconsistent
    - Sometimes the name is in one colum, sometimes in two
    - Address isn't often present, but multiple address columns can exist, such as:
      "Billing Address", "Work Address" and "Home Address".
      Address columns are "* Address [1-2].", "* City", "* State", "* Zip", "* Country".
      Variants include "State/Province" and "Postal Code"
    - Keep only most recent address. This means that input files will need to be processed in order.
      Naming is very inconsistent. I manually fixed a few so that the first year is four digits
      and the only numbers are years. The remaining formats include "2019", "2018-2019", "2018-19",
      "2018-9". The important thing for sorting is that anything with a hypen comes last for that
      first year; the default sort order satisfies this.

Target columns are:
    Name - First
    Name - Last
    Email Address
    Phone Number
    Mailing Address
    City
    State
    Zip
    Alternate name
    Alt email
    Alt phone
"""
from glob import glob
import argparse
import re

import numpy as np
import pandas as pd

COLUMNS = ["Name - First", "Name - Last", "Email Address", "Phone Number", "Mailing Address",
           "City", "State", "Zip", "Alternate name", "Alt email", "Alt phone"]


def make_headers_lowercase(df):
    """
    Make all headers lowercase.
    """
    df.columns = [x.lower().strip().strip("*") for x in df.columns]


def make_first_last(df):
    """
    If the dataframe has only "name", split into "First Name" and "Last Name", adding these two
    columns and splitting the name on a space.
    """
    df.columns = [x.replace("given", "first").replace("family", "last") for x in df.columns]
    if "name" in df.columns or "donor" in df.columns:
        nameseries = df["name"] if "name" in df.columns else df["donor"]
        df["first name"] = nameseries.apply(lambda x: x.split(" ")[0])
        df["last name"] = nameseries.apply(lambda x: " ".join(x.split(" ")[1:]))
        return
    else:
        # handle "Donor First Name" and "Donor Last Name"
        df.columns = [x.replace("donor ", "") for x in df.columns]
        if not "first name" in df.columns:
            raise ValueError("No name in header")


def unify_email(df):
    """
    Any header with email in it will be shortened to "email".
    """
    df.columns = ["email" if "email" in x else x for x in df.columns]
    if not "email" in df.columns:
        return
    df["email"] = df["email"].apply(lambda x: x.strip().replace("+AEA-", "@"))
    df["email"] = df["email"].apply(lambda x: "" if not "@" in x else x)

def format_phone(phone):
    """
    Return phone format 000-000-0000.

    Args:
        phone (str) -- phone number in random format
    """
    formatted = re.sub("\D", "", phone)
    if not formatted:
        return ""
    return "-".join([formatted[:3], formatted[3:6], formatted[6:]])
    
def unify_phone(df):
    """
    Any header with phone in it will be shortened to "phone".

    Returns:
        new df, this cannot make all operations in place
    """
    df.columns = ["phone" if "phone" in x else x for x in df.columns]
    if not "phone" in df.columns:
        return df
    # there is a set of files with some entries containing two phone numbers in a cell
    # these entries are all in the format (000) 000-0000, so create a new row with the second
    # phone number if there are two "(" in the cell. Some entries have text in addition to the
    # numbers, so strip that out. The format function will add in hyphens
    twonums = df["phone"].apply(lambda x: x.count("(") > 1)
    df2 = df[twonums].copy()
    df2["phone"] = df2["phone"].apply(lambda x: re.sub("\D", "", x[15:]))
    df["phone"] = df["phone"].apply(lambda x: x[:15])
    # attempt to unify format
    combined = df.append(df2, ignore_index=True)
    combined["phone"] = combined["phone"].apply(lambda x: format_phone(x))
    return combined


def zip_to_zipcode(df):
    """
    Because zip is special in python, change zip to zip code. Also remove "/postal code".
    """
    df.columns = [x.replace("zip", "zipcode").replace("/postal code", "") for x in df.columns]
    df.columns = [x.replace("postal code", "zipcode") for x in df.columns]
    df.columns = [x.replace("/province", "") for x in df.columns]


def add_addresses(row, table):
    """
    Add addresses from row into table. Names in row should have been unified already with the
    functions above.

    Args:
        row (pd.Series)
        table (Table)
    """
    address_parts = ["address", "city", "state", "zipcode"]
    first = row["first name"]
    last = row["last name"]

    address_names = {x for x in row.index if "address" in x}
    # combine address lines 1 and 2
  
    add = set()
    remove = set()
    for address_name in address_names:
        if "1" in address_name:
            address2 = address_name.replace("1", "2")
            row[address_name.strip(" 1")] = f"{row[address_name]} {row[address2]}"
            add.add(address_name.strip(" 1"))
            remove = remove.union([address_name, address2])
    address_names -= remove
    address_names = address_names.union(add)
        
    for address_name in address_names:
        splitname = address_name.split(" ")
        if len(splitname) == 1:
            address = {ap:row[ap] for ap in address_parts}
        else:
            address = {ap:row[" ".join([splitname[0], ap])] for ap in address_parts}
        table.add(first, last, **address)


class Table:
    """
    Rather than try to do a bunch of complicated merging and de-duping with pandas, the entries
    will be added one at a time to the dataframe. Clarity is more important than efficiency for
    this project. This class will manage the merging.

    Alternate names will not be dealt with here, matching will require manual effort. But the
    column is included so at least the final spreadsheet will have it.
    """
    def __init__(self):
        self.data = pd.DataFrame(columns=COLUMNS)

    def __getitem__(self, row_col_tup):
        """
        Index into self.data with a row index or a tuple of (row_index, column_index).
        """
        return self.data.iloc[row_col_tup]

    def __setitem__(self, row_col_tup, val):
        """
        Set value in self.data with a tuple of (row_index, column_index).
        """
        self.data.iloc[row_col_tup] = val

    def add(self, first=None, last=None, email=None, phone=None, address=None, city=None,
            state=None, zipcode=None):
        """
        Add new entry or information to existing entry if the combination of first and last already
        exist. If last exists with a different first, a message will be printed for a record of
        entries to consider merging.

        If an entry with first and last exists, contact information will be compared and added
        where possible. If all slots are full, a message will be printed.
        """
        firstmatch = self.data["Name - First"].apply(lambda s: s.lower()) == first.lower()
        lastmatch = self.data["Name - Last"].apply(lambda s: s.lower()) == last.lower()
        entrymask = firstmatch & lastmatch
        if not sum(entrymask):
            self.data = self.data.append({COLUMNS[0]:first, COLUMNS[1]:last, COLUMNS[2]:email,
                                          COLUMNS[3]:phone, COLUMNS[4]:address, COLUMNS[5]:city,
                                          COLUMNS[6]:state, COLUMNS[7]:zipcode, COLUMNS[8]:None,
                                          COLUMNS[9]:None, COLUMNS[10]:None}, ignore_index=True)
            return
        if sum(entrymask) > 1:
            raise ValueError(f"multiple entries exist for {first} {last}")
        rowi = np.where(entrymask)[0][0]
        if email and email is not None:
            # no email in existing entry, add new one
            if not self[rowi, 2] or self[rowi, 2] is None:
                self[rowi, 2] = email
            # email doesn't match, save as alt if not taken
            elif self[rowi, 2] != email:
                if self[rowi, -2] is None:
                    self[rowi, -2] = email
                # if the alt exists and is same as new one, continue
                elif self[rowi, -2] != email:
                    print(f"Two emails already exist for {first} {last}, cannot add {email}")

        # repeat for phone
        if phone and phone is not None:
            # no phone in existing entry, add new one
            if not self[rowi, 3] or self[rowi, 3] is None:
                self[rowi, 3] = phone
            # phone doesn't match, save as alt if not taken
            elif self[rowi, 3] != phone:
                if self[rowi, -1] is None:
                    self[rowi, -1] = phone
                # if the alt exists and is same as new one, continue
                elif self[rowi, -1] != phone:
                    print(f"Two phone numbers already exist for {first} {last}, cannot add {phone}")

        # add address, newer address will over-write older one
        if address and not address is None:
            self[rowi, 4] = address
            self[rowi, 5] = city
            self[rowi, 6] = state
            self[rowi, 7] = zipcode


def get_files_in_chron_order(csvfiledir):
    """
    Return list of csv file names in chronological order.

    Args:
        csvfiledir (str) -- name of directory containing csv files.
    """
    files = glob(csvfiledir + "/*csv")
    tosort = []
    for f in files:
        stem = f.split("_")[1].rstrip(".csv")
        noletters = re.sub(r"[A-z]", "", stem)
        tosort.append((noletters.strip("-"), f))
    return [f[1] for f in sorted(tosort)]


def main():
    """
    Process all the files and write data to file.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("csvdir", type=str, help="path to directory containing csv files")
    parser.add_argument("outfile", type=str, help="name, including path, for output csv file")
    args = parser.parse_args()

    table = Table()
    for csvfile in get_files_in_chron_order(args.csvdir):
        print(csvfile)
        curr = pd.read_csv(csvfile)
        curr.fillna("", inplace=True)
        make_headers_lowercase(curr)
        make_first_last(curr)
        unify_email(curr)
        curr = unify_phone(curr)
        zip_to_zipcode(curr)
        for _, row in curr.iterrows():
            email = row["email"] if "email" in row.index else ""
            phone = row["phone"] if "phone" in row.index else ""
            table.add(row["first name"], row["last name"], email.strip(), phone.strip())
            add_addresses(row, table)
    # delete rows with no information other than the name, which tend to be not really people,
    # since spreadsheets have information other than contact information
    # to drop rows, need to use NaN as missing value
    table.data.replace("", np.nan, inplace=True)
    table.data.dropna(how="all", subset=table.data.columns[2:], inplace=True)
    table.data.dropna(how="all", subset=table.data.columns[:2], inplace=True)
    table.data.fillna("")
    table.data.to_csv(args.outfile, index=False)


if __name__ == "__main__":
    main()
