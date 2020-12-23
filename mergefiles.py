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
import numpy as np
import pandas as pd

COLUMNS = ["Name - First", "Name - Last", "Email Address", "Phone Number", "Mailing Address",
           "City", "State", "Zip", "Alternate name", "Alt email", "Alt phone"]


def make_headers_lowercase(df):
    """
    Make all headers lowercase.
    """
    df.columns = [x.lower().strip() for x in df.columns]

def make_first_last(df):
    """
    If the dataframe has only "name", split into "First Name" and "Last Name", adding these two
    columns and splitting the name on a space.
    """
    if "name" in df.columns:
        df["first name"] = df.apply(lambda x: x.split(" ")[0])
        df["last name"] = df.apply(lambda x: " ".join(x.split(" ")[1:]))
        return
    else:
        if not "first name" in df.columns:
            raise ValueError("No name in header")

def unify_email(df):
    """
    Any header with email in it will be shortened to "email".
    """
    df.columns = ["email" if "email" in x else x for x in df.columns]


class Table:
    """
    Rather than try to do a bunch of complicated merging and de-duping with pandas, the entries
    will be added one at a time to the dataframe. This class will manage the merging.

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
        if email is not None:
            # no email in existing entry, add new one
            if self[rowi, 2] is None:
                self[rowi, 2] = email
            # email doesn't match, save as alt if not taken
            elif self[rowi, 2] != email:
                if self[rowi, -2] is None:
                    self[rowi, -2] = email
                # if the alt exists and is same as new one, continue
                elif self[rowi, -2] != email:
                    print(f"Two emails already exist for {first} {last}, cannot add {email}")

        # repeat for phone
        if phone is not None:
            # no phone in existing entry, add new one
            if self[rowi, 3] is None:
                self[rowi, 3] = phone
            # phone doesn't match, save as alt if not taken
            elif self[rowi, 3] != phone:
                if self[rowi, -1] is None:
                    self[rowi, -1] = phone
                # if the alt exists and is same as new one, continue
                elif self[rowi, -1] != phone:
                    print(f"Two phone numbers already exist for {first} {last}, cannot add {phone}")

        # add address, newer address will over-write older one
        self[rowi, 4] = address
        self[rowi, 5] = city
        self[rowi, 6] = state
        self[rowi, 7] = zipcode
