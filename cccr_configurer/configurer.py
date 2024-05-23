import argparse
import csv
import logging
import os

from p4p.client.thread import Context

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

# TODO: check for CUSTNAME duplicates
# TODO: get values -> output table
# TODO: ignore cells with NONE or blanks
# TODO: validate input against domain type and raise exceptions
# TODO: update CSV (IDLINE5 -> DESC5, SPECDATATYP -> SDTYP)
# TODO: differentiate between debugging and info to print

record_pattern = "FDAS:<CHASSIS>:SA:Ch<CHANNEL>:<DOMAIN>"
alarm_pattern = "FDAS:<CHASSIS>:ACQ:<DOMAIN>:<CHANNEL>"
filename_record = "FDAS:SA:FILE"

name_dict = {
    # Overall System CH #
    "SIGNAL": {
        "type": "int",
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Digitizer #
    "CHASSIS": {
        "type": "int",
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Digitizer Channel #
    "CHANNEL": {
        "type": "int",
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Digitizer Connector #
    "CONNECTOR": {
        "type": "str",  # [DB1, DB2]
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
}

# The default valid_input must be the first element of the list
# These are columns in the CSV, which EPICS signal pattern they follow, valid inputs
# and space for their ultimate pv names and values to put into runtime database
records_dict = {
    # Channel Use (Yes/No)
    "USE": {
        "type": "bool",  # EPICS:enum [Yes, No]
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Channel User Label
    "CUSTNAM": {
        "type": "str",  # Full Channel Name with Customer-requested designator
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Channel Description
    "DESC": {
        "type": "str",  # UFF58 ID Line 2
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    "IDLINE5": {
        "type": "str",  # UFF58 ID Line 5
        "pattern": record_pattern.replace("<DOMAIN>", "DESC5"),
        "pv_names": [],
        "pv_values": [],
    },
    "RESPNODE": {
        "type": "int",  # UFF58 ID Line 6, field 6
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Response Direction
    "RESPDIR": {
        "type": "str",  # UFF58 ID Line 6, field 6
        "pattern": record_pattern,
        "input_switch": {  # with mbbo values
            "scalar": "Scalar",  # 0, default
            "default": "Scalar",
            "X+": "+X Translation",  # 1
            "Y+": "+Y Translation",  # 2
            "Z+": "+Z Translation",  # 3
            "X-": "-X Translation",  # -1
            "Y-": "-Y Translation",  # -2
            "Z-": "-Z Translation",  # -3
            "XR+": "+X Rotation",  # 4
            "YR+": "+Y Rotation",  # 5
            "ZR+": "+Z Rotation",  # 6
            "XR-": "-X Rotation",  # -4
            "YR-": "-Y Rotation",  # -5
            "ZR-": "-Z Rotation",  # -6
        },
        # "valid_inputs": list(records_dict["RESPDIR"]["input_switch"].keys())
        "pv_names": [],
        "pv_values": [],
    },
    # Specific Data Type Field
    "SPECDATATYP": {
        "type": "str",  # UFF58 ID Line 6, field 6
        "pattern": record_pattern.replace("<DOMAIN>", "SDTYP"),
        "valid_input": [
            "unknown",  # 0, default value
            "general",
            "stress",
            "strain",
            "temperature",
            "heat flux",
            "displacement",
            "reaction force",
            "velocity",
            "acceleration",
            "excitation force",
            "pressure",
            "mass",
            "time",
            "frequency",
            "rpm",
        ],
        "pv_names": [],
        "pv_values": [],
    },
    # Engineering Unit
    "EGU": {
        "type": "str",
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Custom Measurement Location
    # CUSTMEASLOC str
    # Volts to EU slope
    "ESLO": {
        "type": "float",  # EGU/V
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Volts to EU offset
    "EOFF": {
        "type": "float",  # EGU
        "pattern": record_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # MAXEULVL float
    # SAMPLPERSEC int
    # Low Alarm Limit (in EU)
    "LOLOlim": {
        "type": "float",  # EGU
        "pattern": alarm_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Low Warning Level (in EU)
    "LOlim": {
        "type": "float",  # EGU
        "pattern": alarm_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # High Warning Level (in EU)
    "HIlim": {
        "type": "float",  # EGU
        "pattern": alarm_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # High Alarm Limit (in EU)
    "HIHIlim": {
        "type": "float",  # EGU
        "pattern": alarm_pattern,
        "pv_names": [],
        "pv_values": [],
    },
    # Coupling (AC or DC)
    # "COUPLING": {
    # "type": "str",  # EPICS:enum [AC, DC]
    # "pattern": record_pattern,
    # "pv_names": [],
    # "pv_values": [],
    # },
    # Configuration Timestamp
    # CONFIGTIMEID str
}

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


def convert_type(description, val, domain_type: str):
    """Converts the data type of the given value based on the record's domain's type.
    Specifically designed

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
    logging.debug(f"Converting {description} to {domain_type}")
    if val is None or len(val) == 0:
        return None
    elif domain_type == "int":
        return 0 if val.upper() == "NONE" else int(val)
    elif domain_type == "str":
        return str(val).strip()
    elif domain_type == "float":
        return 0 if val.upper() == "NONE" else float(val)
    elif domain_type == "bool":
        if val.lower() == "yes":
            return "Yes"
        elif val.lower() == "no":
            return "No"
    raise ValueError("Record is not within the scope of configurator")


def verify_input(description: str, val: str, valid_inputs: list) -> str:
    """Verifies the given input data is within the valid input list, otherwise returns
    default.

    params
    ----------
    description : str
        User provided name to alert of value mismatch

    val : str
        String value from the CCCR to check

    valid_inputs : list
        List of valid inputs

    returns
    -------
    valid_value[0] : str
        First matched valid input, or default valid input
    """
    logging.debug(f"Verifying input for {description}")
    valid_value = [option.lower() for option in valid_inputs if val == option.lower()]
    return valid_value[0] if valid_value else valid_inputs[0]


def apply_input_switch(description: str, val: str, input_switch: dict) -> str:
    """Switches CSV input to EPICS DB input, if a switch exists for that domain.

    params
    ----------
    description : str
        User provided name to alert of value mismatch

    val : str
        String value from the CCCR to check

    input_switch : dict
        Input switch

    returns
    -------
    switched_val : str
        Value switched from CSV standard to EPICS DB standard
    """
    logging.debug(f"{input_switch}")
    logging.debug(f"Applying input switch for {description}")
    if val in list(input_switch.keys()):
        switched_val = input_switch[val]
    else:
        switched_val = input_switch["default"]
    return switched_val


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

    # logging.info(f"{domain_type}:{val} converted to {ret}")
    return ret


configuration = {}
args = get_input_arguments()
logging.getLogger("p4p").setLevel(logging.WARNING)

if args.verbose:
    logging.basicConfig(
        format="%(levelname)s: %(asctime)s - %(message)s", level=logging.DEBUG
    )
    logging.info("Verbose output")
else:
    logging.basicConfig(format="%(levelname)s: %(message)s")

logging.info(f"Arguments: {args.__dict__}")
logging.debug(
    f"Input filepath {args.input_filepath}; string: {isinstance(args.input_filepath, str)}"
)

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
        raise FileNotFoundError(f"File {args.input_filepath} not found")
    elif not os.path.isdir(args.output_path):
        raise FileNotFoundError(f"Path {args.output_path} not found")
    else:
        configuration["input_fp"] = args.input_filepath
        configuration["output_fp"] = args.output_path + "/output.csv"

logging.info(f"{configuration}")
logging.info(f"Input file: {configuration['input_fp']}")
logging.info(f"Output file: {configuration['output_fp']}")
logging.info(f"Domains to output: {domains}")
logging.info(f"Record pattern: {record_pattern}")
logging.info(f"Alarm pattern: {alarm_pattern}")

output_table = []

"""Open the input filepath, read it, convert the values, and append the new row
to the output file."""
logging.info("Convert values to accepted datatypes and format")
with open(configuration["input_fp"], newline="") as configuration_csv:
    configuration_table = csv.DictReader(configuration_csv)
    # headers = configuration_table.keys()
    # logging.info("Headers: {}".format(headers))

    for row in configuration_table:
        channel_desc = row["DESC"]
        for key, value in row.items():
            if key in domains and row["USE"].lower() == "yes":
                value = convert_type(
                    channel_desc + ":" + key,
                    value,
                    records_dict[key]["type"],
                )
                if "valid_input" in records_dict[key]:
                    value = verify_input(
                        channel_desc + ":" + key,
                        value,
                        records_dict[key]["valid_input"],
                    )
                if "input_switch" in records_dict[key]:
                    value = apply_input_switch(
                        channel_desc + ":" + key,
                        value,
                        records_dict[key]["input_switch"],
                    )
                row[key] = value
        output_table.append(row)


# logging.info("Output table ({}): {}".format(type(output_table), output_table))

"""Using output table (with converted values), traverse. Construct channel names and
append domains from the headers to construct signal names. Use signal names and values
to put into EPICS DB. Revert the value to string within the output."""
logging.info("Begin configuration put to database")
ctxt = Context("pva")
for row in output_table:
    if row["USE"].lower() == "yes":
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
        # pass
        pv_name = str(
            records_dict["USE"]["pattern"]
            .replace("<CHASSIS>", f"{int(row['CHASSIS']):02d}")
            .replace("<CHANNEL>", f"{int(row['CHANNEL']):02d}")
        ).replace("<DOMAIN>", "USE")
        records_dict["USE"]["pv_names"].append(pv_name)
        records_dict["USE"]["pv_values"].append("No")
    else:
        raise ValueError(f"Not among allowed values {row['USE']}")


for domain in records_dict:
    logging.debug(
        f"Putting {domain}...\n {list(zip(records_dict[domain]['pv_names'], records_dict[domain]['pv_values']))}"
    )
    try:
        ctxt.put(records_dict[domain]["pv_names"], records_dict[domain]["pv_values"])
    except:
        logging.exception("While putting %s", domain)
        raise

logging.debug(os.path.basename(configuration["input_fp"]))
ctxt.put(filename_record, os.path.basename(configuration["input_fp"]))

logging.info("Write output configuration file")
fieldnames = output_table[0].keys()
logging.info(f"{list(fieldnames)}")
with open(configuration["output_fp"], "w", newline="") as output_file:
    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
    writer.writeheader()
    for row in output_table:
        writer.writerow(row)
