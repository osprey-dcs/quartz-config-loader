import argparse
import csv
import hashlib
import logging
import os

from p4p.client.thread import Context

# TODO: convert records_dict to an OrderedDict?
# TODO: write PVs in batches put(array, array)

"""Reads the CCCR Configuration csv file.

    Input
    ----------
    input_filepath : str
        The file location of the CCCR csv

    output_filepath : str
        The file location to write output CCCR csv

    Output
    -------
    CCCR Output File
        A csv of the values that were used to _put_ into the EPICS DB
"""

record_pattern = "FDAS:<CHASSIS>:SA:Ch<CHANNEL>:<DOMAIN>"
alarm_pattern = "FDAS:<CHASSIS>:ACQ:<DOMAIN>:<CHANNEL>"

name_dict = {
    "SIGNAL": {
        "type": "int",
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "CHASSIS": {
        "type": "int",
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "CHANNEL": {
        "type": "int",
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "CONNECTOR": {
        "type": "str",  # [DB1, DB2]
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # CUSTNAM str
}
records_dict = {
    "USE": {
        "type": "bool",  # EPICS:enum [Yes, No]
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "CUSTNAM": {
        "type": "str",  # Full Channel Name with Customer-requested designator
        "pattern": "FDAS:<CHASSIS>:SA:Ch<CHANNEL>:NAME",
        "pv_names": [],
        "pv_values": [],
    },
    "DESC": {
        "type": "str",  # UFF58 ID Line 2
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "IDLINE5": {
        "type": "str",  # UFF58 ID Line 5
        "pattern": "FDAS:<CHASSIS>:SA:Ch<CHANNEL>:DESC5",
        "pv_names": [],
        "pv_values": [],
    },
    "RESPNODE": {
        "type": "int",  # UFF58 ID Line 6, field 6
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    #"RESPDIR": {
        #"type": "str",  # UFF58 ID Line 6, field 6
        #"pattern": record_pattern,
        #"pv_names": [],
        #"pv_values": [],
    #},
    #"SPECDATATYP": {
        #"type": "int",  # UFF58 ID Line 6, field 6
        #"pattern": "FDAS:<CHASSIS>:SA:Ch<CHANNEL>:SDTYP",
        #"pv_names": [],
        #"pv_values": [],
    #},
    "EGU": {
        "type": "str",
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # CUSTMEASLOC str
    "ESLO": {
        "type": "float",  # EGU/V
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "EOFF": {
        "type": "float",  # EGU
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # MAXEULVL float
    # SAMPLPERSEC int
    "LOLOlim": {
        "type": "float",  # EGU
        "pattern": alarm_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "LOlim": {
        "type": "float",  # EGU
        "pattern": alarm_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "HIlim": {
        "type": "float",  # EGU
        "pattern": alarm_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "HIHIlim": {
        "type": "float",  # EGU
        "pattern": alarm_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    #"COUPLING": {
        #"type": "str",  # EPICS:enum [AC, DC]
        #"pattern": record_pattern,
        #"pv_names": [],
        #"pv_values": [],
    #},
    # CONFIGTIMEID str
}

# Global PVs to write CCCR.csv filename, and input_hash
# TODO copy the input_csv to a separate directory (need destination from Davdisaver)

domains = list(records_dict.keys())


def get_input_arguments():
    parser = argparse.ArgumentParser(description="Process csv configuration file")
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        dest="input_filepath",
        required=False,
        help="Input file's filepath",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        dest="output_path",
        required=False,
        help="Path to the output folder",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        required=False,
        help="Run in test mode with dummy values from tests/input/dummy.csv",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        required=False,
        help="Run in verbose mode",
    )

    return parser.parse_args()


def convert_value(description, val, domain_type: str):
    """Converts the data type of the given value based on the record's domain's type.

    params
    ----------
    description : str
        User provided name to alert of value mismatch

    val : any
        Value from the CCCR to convert

    domain_type : str
        The record's domain's data type to convert to

    returns
    -------
    val : [integer, string, float, boolean]
        Returned value of new data type according to the list available
    """
    # logging.info("Converting {} to {}".format(description, domain))
    if val is None or len(val) == 0:
        return None
    elif domain_type == "int":
        return 0 if val.upper()=='NONE' else int(val)
    elif domain_type == "str":
        return str(val).strip()
    elif domain_type == "float":
        return float(val)
    elif domain_type == "bool":
        if val.lower() == "yes":
            return "Yes"
        elif val.lower() == "no":
            return "No"
    raise ValueError("Record is not within the scope of configurator")


def revert_value(val, domain_type: str):
    """Reverts the data type of the given value to a string.

    params
    ----------
    val : any
        Value to revert to string

    domain_type : str
        The record's domain's data type to convert from. Primarily to revert
        boolean data.

    returns
    -------
    ret : string
        String representation of the given val
    """
    if val is None:
        ret = ""
    elif domain_type == "bool":
        return "Yes" if val else "No"
    else:
        ret = str(val)

    # logging.info("{}:{} converted to {}".format(domain_type, val, ret))
    return ret


configuration = {}
args = get_input_arguments()
logging.getLogger("p4p").setLevel(logging.WARNING)

if args.verbose:
    logging.basicConfig(
        format="%(levelname)s: %(asctime)s - %(message)s", level=logging.DEBUG
    )
    logging.info("Verbose output.")
else:
    logging.basicConfig(format="%(levelname)s: %(message)s")

logging.info("Arguments:{}".format(args))

if args.test:
    logging.info("Entering test mode")

    tests_path = os.path.dirname(os.path.realpath(__file__)) + "/tests"
    configuration["input_fp"] = tests_path + "/input/" + "dummy.csv"
    configuration["output_fp"] = tests_path + "/output/" + "output.csv"

elif args.input_filepath is None:
    print("No input filepath selected")
    exit()

else:
    if not os.path.exists(args.input_filepath):
        raise FileNotFoundError("File {} not found".format(args.input_filepath))
    elif not os.path.isdir(args.output_path):
        raise FileNotFoundError("Path {} not found".format(args.output_path))
    else:
        configuration["input_fp"] = args.input_filepath
        configuration["output_fp"] = args.output_path + "/output.csv"

logging.info("Input file: {}".format(configuration["input_fp"]))
logging.info("Output file: {}".format(configuration["output_fp"]))
logging.info("Domains to output: {}".format(domains))
logging.info("Record pattern: {}".format(record_pattern))
logging.info("Alarm pattern: {}".format(alarm_pattern))

output_table = []

# TODO: check for CUSTNAME duplicates

"""Open the input filepath, read it, convert the values, and append the new row
to the output file."""
with open(configuration["input_fp"], newline="") as configuration_csv:
    configuration_table = csv.DictReader(configuration_csv)
    # headers = configuration_table.keys()
    # logging.info("Headers: {}".format(headers))

    for row in configuration_table:
        channel_desc = row["DESC"]
        for key, value in row.items():
            if key in domains:
                value = convert_value(
                    channel_desc + ":" + key,
                    value,
                    records_dict[key]["type"],
                )
                row[key] = value
        output_table.append(row)


#logging.info("Output table ({}): {}".format(type(output_table), output_table))

"""Using output table (with converted values), traverse. Construct channel names and
append domains from the headers to construct signal names. Use signal names and values
to put into EPICS DB. Revert the value to string within the output."""
logging.info("Begin configuration put to database")
ctxt = Context("pva")
for row in output_table:
    if row["USE"].lower()=="yes":
        for key, value in row.items():
            if key in domains:
                pv_name = str(
                    records_dict[key]["pattern"]
                    .replace("<CHASSIS>", "{:02d}".format(int(row["CHASSIS"])))
                    .replace("<CHANNEL>", "{:02d}".format(int(row["CHANNEL"])))
                ).replace("<DOMAIN>", key)

                records_dict[key]["pv_names"].append(pv_name)
                records_dict[key]["pv_values"].append(value)
                row[key] = revert_value(value, records_dict[key]["type"])
    elif row["USE"].lower() == "no":
        pv_name = str(
                    records_dict["USE"]["pattern"]
                    .replace("<CHASSIS>", "{:02d}".format(int(row["CHASSIS"])))
                    .replace("<CHANNEL>", "{:02d}".format(int(row["CHANNEL"])))
                ).replace("<DOMAIN>", "USE")
        records_dict["USE"]["pv_names"].append(pv_name)
        records_dict["USE"]["pv_values"].append("No")
    else:
        raise ValueError("Not among allowed values {}".format(row["USE"]))


for domain in records_dict:
    #logging.info(
        #"Putting {}...\n {}".format(
            #domain,
            #list(zip(records_dict [domain]["pv_names"], records_dict[domain]["pv_values"]))
        #)
    #)
    try:
        ctxt.put(records_dict[domain]["pv_names"], records_dict[domain]["pv_values"])
    except:
        logging.exception('While PUTing %s', domain)
        raise

logging.info("Write output configuration file")
fieldnames = output_table[0].keys()
logging.info(f'{fieldnames}')
with open(configuration["output_fp"], "w", newline="") as output_file:
    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
    writer.writeheader()
    for row in output_table:
        writer.writerow(row)


def hashfile(file):
    """Creates hash string from file, passing data through buffer first.

    params
    ----------
    file
        File from which to create hash

    returns
    -------
    string
        String representation hash
    """
    # Arbitrary buffer size: 65536 bytes = 64 kilobytes
    BUF_SIZE = 65536

    # Initializing the sha256() method
    sha256 = hashlib.sha256()

    with open(file, "rb") as f:
        while True:
            # reading data = BUF_SIZE from the file and saving it in a variable
            data = f.read(BUF_SIZE)

            # True if eof = 1
            if not data:
                break

            # Passing data to sha256 hash function
            sha256.update(data)

    # sha256.hexdigest() hashes all the input data hashing the data,
    # and returns the output in hexadecimal format
    return sha256.hexdigest()


# Obtain hashes of files
input_hash = hashfile(configuration["input_fp"])
output_hash = hashfile(configuration["output_fp"])

# Doing string comparison to check whether the two hashes match or not
#if input_hash == output_hash:
    #print("Both files are same")
    #print(f"Hash: {input_hash}")

#else:
    #print("Files are different!")
    #print(f"Hash of Input: {input_hash}")
    #print(f"Hash of Output: {output_hash}")


# Function to compare two CSV files
def compare(file1, file2):
    """Compares two csv files by comparing each line as a string

    params
    ----------
    file1, file2
        Files to compare

    returns
    -------
    list
        List of strings of different rows
    """
    differences = []

    # Open both CSV files in read mode
    with open(file1, "r") as csv_file1, open(file2, "r") as csv_file2:
        reader1 = csv.reader(csv_file1)
        reader2 = csv.reader(csv_file2)

        # Iterate over rows in both files simultaneously
        for row1, row2 in zip(reader1, reader2):
            # for val1, val2 in zip(row1, row2):
            #     print(f"{val1}=={val2} :" + str((val1 == val2)))
            if row1 != row2:
                differences.append((row1, row2))

    return differences


# Call the compare_csv_files function and store the differences
#differences = compare(configuration["input_fp"], configuration["output_fp"])
#for diff in differences:
    #print(f"Difference found: {diff}")
