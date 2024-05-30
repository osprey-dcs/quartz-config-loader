import csv
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET

from p4p.client.thread import Context

"""Reads the CCCR Configuration csv file.

    Parameters:
    input_filepath (str): The file location of the CCCR csv
    output_filepath (str): The file location to write output CCCR csv

    Returns:
    output_file (.csv): A csv of the values that were used to _put_ into the EPICS DB
"""

_log = logging.getLogger(__name__)

# TODO: check for CUSTNAME duplicates
# TODO: get values -> output table
# TODO: ignore cells with NONE or blanks
# TODO: validate input against domain type and raise exceptions
# TODO: differentiate between debugging and info to print
# CONSIDER: update CSV (IDLINE5 -> DESC5, SPECDATATYP -> SDTYP)

SIGNAL_PATTERN = "S<SIGNAL>:CS<CHASSIS>:CH<CHANNEL>:CN<CONNECTOR>"
RECORD_PATTERN = "FDAS:<CHASSIS>:SA:Ch<CHANNEL>:<DOMAIN>"
ALARM_PATTERN = "FDAS:<CHASSIS>:ACQ:<DOMAIN>:<CHANNEL>"
FILENAME_RECORD = "FDAS:SA:FILE"

# The default valid_input must be the first element of the list
# These are columns in the CSV, which EPICS signal pattern they follow, valid inputs
# and space for their ultimate pv names and values to put into runtime database
NAME_METADATA = {
    # Overall System CH #
    "SIGNAL": {
        "type": "int",
    },
    # Digitizer #
    "CHASSIS": {
        "type": "int",
    },
    # Digitizer Channel #
    "CHANNEL": {
        "type": "int",
        # Digitizer Connector #
    },
    "CONNECTOR": {
        "type": "str",  # [DB1, DB2]
    },
}

DOMAINS = {
    # Channel Use (Yes/No)
    "USE": {
        "type": "bool",  # EPICS:enum [Yes, No]
        "pattern": RECORD_PATTERN,
    },
    # Channel User Label
    "CUSTNAM": {
        "type": "str",  # Full Channel Name with Customer-requested designator
        "pattern": RECORD_PATTERN.replace("<DOMAIN>", "NAME"),
    },
    # Channel Description
    "DESC": {
        "type": "str",  # UFF58 ID Line 2
        "pattern": RECORD_PATTERN,
    },
    "IDLINE5": {
        "type": "str",  # UFF58 ID Line 5
        "pattern": RECORD_PATTERN.replace("<DOMAIN>", "DESC5"),
    },
    "RESPNODE": {
        "type": "int",  # UFF58 ID Line 6, field 6
        "pattern": RECORD_PATTERN,
    },
    # Response Direction
    "RESPDIR": {
        "type": "str",  # UFF58 ID Line 6, field 6
        "pattern": RECORD_PATTERN,
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
        # "valid_input": list(DOMAINS["RESPDIR"]["input_switch"].keys())
    },
    # Specific Data Type Field
    "SPECDATATYP": {
        "type": "str",  # UFF58 ID Line 6, field 6
        "pattern": RECORD_PATTERN.replace("<DOMAIN>", "SDTYP"),
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
    },
    # Engineering Unit
    "EGU": {
        "type": "str",
        "pattern": RECORD_PATTERN,
    },
    # Custom Measurement Location
    # CUSTMEASLOC str
    # Volts to EU slope
    "ESLO": {
        "type": "float",  # EGU/V
        "pattern": RECORD_PATTERN,
    },
    # Volts to EU offset
    "EOFF": {
        "type": "float",  # EGU
        "pattern": RECORD_PATTERN,
    },
    # MAXEULVL float
    # SAMPLPERSEC int
    # Low Alarm Limit (in EU)
    "LOLOlim": {
        "type": "float",  # EGU
        "pattern": ALARM_PATTERN,
    },
    # Low Warning Level (in EU)
    "LOlim": {
        "type": "float",  # EGU
        "pattern": ALARM_PATTERN,
    },
    # High Warning Level (in EU)
    "HIlim": {
        "type": "float",  # EGU
        "pattern": ALARM_PATTERN,
    },
    # High Alarm Limit (in EU)
    "HIHIlim": {
        "type": "float",  # EGU
        "pattern": ALARM_PATTERN,
    },
    # Coupling (AC or DC)
    # "COUPLING": {
    # "type": "str",  # EPICS:enum [AC, DC]
    # "pattern": RECORD_PATTERN,
    # },
    # Configuration Timestamp
    # CONFIGTIMEID str
}

DOMAINS_LIST = list(DOMAINS.keys())


def getargs():
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Process csv configuration file")
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
        dest="level",
        default=logging.INFO,
        action="store_const",
        const=logging.DEBUG,
        required=False,
        help="Run in verbose mode",
    )
    parser.add_argument(
        "--sim", action="store_true", help="Do not actually change any PVs."
    )

    return parser


def convert_bytype(description: str, val, domain_type: str):
    """Converts the data type of the given value based on the record's domain's type.
    Specifically designed.

    Parameters
    description (str): User provided name to alert of value mismatch
    val (any): Value from the CCCR to convert
    domain_type (str): The record's domain's data type to convert to

    Returns
    val ([integer, string, float, boolean]): Returned value of new data type according
    to the list available
    """
    # _log.debug(f"Converting {description}: {val} to {domain_type}")
    if val is None or len(val) == 0:
        # _log.debug("val is none")
        return None
    elif domain_type == "int":
        # _log.debug("val is int")
        return 0 if val.upper() == "NONE" else int(val)
    elif domain_type == "str":
        # _log.debug("val is str")
        return str(val).strip()
    elif domain_type == "float":
        # _log.debug("val is float")
        return 0 if val.upper() == "NONE" else float(val)
    elif domain_type == "bool":
        # _log.debug("val is bool")
        if val.lower() == "yes":
            return "Yes"
        elif val.lower() == "no":
            return "No"
    raise ValueError(f"Could not convert '{description}:{val}'")


def verify_input(description: str, val: str, valid_inputs: list) -> str:
    """Verifies the given input data is within the valid input list, otherwise returns
    default.

    Parameters
    description (str): User provided name to alert of value mismatch
    val (str): String value from the CCCR to check
    valid_inputs (list): List of valid inputs

    Returns
    valid_value[0] (str): First matched valid input, or default valid input
    """
    # _log.debug(f"Verifying input for {description}")
    valid_value = [option.lower() for option in valid_inputs if val == option.lower()]
    return valid_value[0] if valid_value else valid_inputs[0]


def apply_input_switch(description: str, val: str, input_switch: dict) -> str:
    """Switches CSV input to EPICS DB input, if a switch exists for that domain.

    Parameters
    description (str): User provided name to alert of value mismatch
    val (str): String value from the CCCR to check
    input_switch (dict): Input switch

    Returns
    switched_val (str): Value switched from CSV standard to EPICS DB standard
    """
    # _log.debug(f"Applying input switch for {description}")
    if val in list(input_switch.keys()):
        switched_val = input_switch[val]
    else:
        switched_val = input_switch["default"]
    return switched_val


class Signal:
    def __init__(self, config: dict):
        self.name = (
            SIGNAL_PATTERN.replace("<SIGNAL>", config["SIGNAL"])
            .replace("<CHASSIS>", config["CHASSIS"])
            .replace("<CHANNEL>", config["CHANNEL"])
            .replace("<CONNECTOR>", config["CONNECTOR"])
        )
        self.name_dict = {m: config[m] for m in NAME_METADATA.keys()}
        self.config: dict = config  # csv cfg file row
        self.use: str = config["USE"].lower()
        if self.use == "yes":
            self.records: dict = {
                key: Record(
                    signal=self,
                    signal_cfg=config,
                    domain=key,
                    chassis=config["CHASSIS"],
                    channel=config["CHANNEL"],
                    cfg_value=value,
                )
                for key, value in config.items()  # for each column in cfg file
                if key in DOMAINS_LIST  # if the header appears in the domains list
            }
        else:
            for d in DOMAINS_LIST:
                config[d] = None

    def signal_todict(self):
        signal_dict = self.name_dict
        records_dict = {}
        if hasattr(self, "records"):
            # records_dict = {k: r.new_value.value for k, r in self.records.items()}
            for k, r in self.records.items():
                # _log.debug(f"{self.name} {k} {r.new_value}")
                records_dict.update({k: r.value})
        else:
            records_dict = self.config
        signal_dict.update(records_dict)
        # _log.debug(f"{signal_dict}")
        return signal_dict


class Record:
    def __init__(
        self,
        signal: Signal,
        signal_cfg: dict,
        domain: str,
        chassis: str,
        channel: str,
        cfg_value,
    ):
        self.signal: Signal = signal
        self.signal_cfg = signal_cfg
        self.rec_name: str = str(
            DOMAINS[domain]["pattern"]
            .replace("<CHASSIS>", "{:02d}".format(int(chassis)))
            .replace("<CHANNEL>", "{:02d}".format(int(channel)))
        ).replace("<DOMAIN>", domain)
        self.value = self.process_val(
            signal_cfg["DESC"] + f":{domain}", cfg_value, domain
        )
        self.old_value = None  # get from EPICS DB
        self.changed: bool = False  # when new_value put, compare
        self._new_value = None  # get after put
        self.domain = domain

    @property
    def new_value(self):
        return self._new_value

    @new_value.setter
    def new_value(self, value):
        self._new_value = value
        self.changed = self._new_value != self.old_value

    def __setattr__(self, key, value):
        if key == "new_value":
            self._new_value = value
            self.changed = self._new_value != self.old_value
        else:
            super().__setattr__(key, value)

    def process_val(self, description: str, value, domain: str):
        cfg_value = value
        # _log.debug(
        #     f"converting {description} ({domain}): {DOMAINS[domain]['type']}({value})"
        # )
        if "valid_input" in DOMAINS[domain]:
            cfg_value = verify_input(
                description,
                cfg_value,
                DOMAINS[domain]["valid_input"],
            )
        if "input_switch" in DOMAINS[domain]:
            cfg_value = apply_input_switch(
                description,
                cfg_value,
                DOMAINS[domain]["input_switch"],
            )

        cfg_value = convert_bytype(
            description=self.signal_cfg["DESC"] + f":{domain}",
            val=cfg_value,
            domain_type=DOMAINS[domain]["type"],
        )

        # _log.debug(f"new value: {cfg_value}")
        return cfg_value


def main():
    configuration = {}
    signals = []

    args = getargs().parse_args()

    logging.getLogger("p4p").setLevel(logging.WARNING)
    logging.basicConfig(
        level=args.level, format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
    )

    _log.info(f"Arguments: {args.__dict__}")
    _log.debug(
        f"Input filepath {args.input_filepath}; string: {isinstance(args.input_filepath, str)}"
    )

    if args.test:
        _log.info("Entering test mode")

        tests_path = os.path.dirname(os.path.realpath(__file__)) + "/tests"
        configuration["input_fp"] = tests_path + "/input/" + "test.csv"
        configuration["output_fp"] = tests_path + "/output/" + "output.csv"

    elif args.input_filepath is None:
        print("No input filepath selected")
        sys.exit(0)

    else:
        if not os.path.exists(args.input_filepath):
            raise FileNotFoundError(f"File {args.input_filepath} not found")
        elif not os.path.isdir(args.output_path):
            raise FileNotFoundError(f"Path {args.output_path} not found")
        else:
            configuration["input_fp"] = args.input_filepath
            configuration["output_fp"] = args.output_path + "/output.csv"

    input_fn_with_ext = os.path.basename(configuration["input_fp"])
    inputfn, inputfn_ext = os.path.splitext(input_fn_with_ext)
    configuration["filename"] = inputfn
    _log.info(f"Input file: {configuration['input_fp']}")
    _log.info(f"Output file: {configuration['output_fp']}")
    _log.info(f"Domains to output: {DOMAINS_LIST} ({len(DOMAINS_LIST)})")
    _log.debug(f"Record pattern: {RECORD_PATTERN}")
    _log.debug(f"Alarm pattern: {ALARM_PATTERN}")

    """Open the input filepath, read it, convert the values, and append the new row
    to the output file."""
    _log.info("Converting values to accepted datatypes and format")
    with open(configuration["input_fp"], newline="") as configuration_csv:
        configuration_table = csv.DictReader(configuration_csv)
        for row in configuration_table:
            s = Signal(dict(row))
            signals.append(s)
        headers = configuration_table.fieldnames

    # Organize the records by Domain
    recs_bydomain = {d: [] for d in DOMAINS_LIST}
    for dom in DOMAINS_LIST:
        for s in [s for s in signals if hasattr(s, "records")]:
            for d, r in s.records.items():
                if d == dom:
                    recs_bydomain[dom].append(r)

    if args.sim:
        time.sleep(5)  # fake some actual work
        _log.warning("Simulation")
        sys.exit(0)

    ctxt = Context("pva")
    count_changed = 0
    recs_changed = []
    for d, recs in recs_bydomain.items():
        rec_names = [r.rec_name for r in recs]
        rec_vals = [r.value for r in recs]

        _log.info(f"Processing {d}")
        # _log.info(f"Processing {d}: {list(zip(rec_names,rec_vals))}")
        # _log.info("GET old values")
        old_values = ctxt.get(rec_names)
        for rec, old_value in zip(recs, old_values):
            rec.old_value = old_value

        # _log.info("PUT config values")
        ctxt.put(rec_names, rec_vals)

        # _log.info("GET new values")
        new_values = ctxt.get(rec_names)
        for rec, new_value in zip(recs, new_values):
            rec.new_value = new_value
            if rec.changed:
                count_changed += 1
                recs_changed.append(rec.rec_name)
            # _log.debug(
            #     f"{rec.rec_name}, {rec.old_value}, {rec.new_value}, {rec.changed}"
            # )

    _log.info(f"Records changed: {count_changed}")
    # _log.debug(f"records changed: {recs_changed}")
    _log.info("Write output configuration file")
    with open(configuration["output_fp"], "w", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=headers)
        writer.writeheader()
        for s in signals:
            # _log.debug(f"Writing: {s.name}")
            writer.writerow(s.signal_todict())

    # Setup XML Tree
    def indent(elem, level=0):
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for elem in elem:
                indent(elem, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i

    pvtable = ET.Element("pvtable", enable_save_restore="true", version="3.0")
    timeout = ET.SubElement(pvtable, "timeout")
    timeout.text = "60.0"
    pvlist = ET.SubElement(pvtable, "pvlist")
    for s in signals:
        if hasattr(s, "records"):
            for k, r in s.records.items():
                # _log.debug(f"add {r.rec_name} to xml")
                pv = ET.SubElement(pvlist, "pv")
                ET.SubElement(pv, "name").text = str(r.rec_name)
                ET.SubElement(pv, "saved_value").text = str(r.value)
    # _log.debug("Make tree")
    indent(pvtable)
    tree = ET.ElementTree(pvtable)
    output_xml_fp = os.path.dirname(configuration["output_fp"]) + "/output.xml"
    _log.info("Write xml file")

    with open(output_xml_fp, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True)


if __name__ == "__main__":
    main()
